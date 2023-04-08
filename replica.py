import socket
import select
import sys
from _thread import *
from threading import Lock
import re
import threading
import csv
import os

NUM_MACHINES = 3
ADDR_1 = ""
ADDR_2 = ""
ADDR_3 = ""

PORT_1 = ""
PORT_2 = ""
PORT_3 = ""


# IP address is first argument
IP = str(sys.argv[1])


# Port number is second argument
port = int(sys.argv[2])

# Machine number
machine_idx = str(sys.argv[3])

# maintains a list of potential clients
list_of_clients = []

# client username dictionary, with login status: 0 if logged off, corresponding address if logged in
client_dictionary = {}


# replica dictionary, keyed by address and valued at machine id
replica_dictionary = {"1" : (ADDR_1, PORT_1), "2" : (ADDR_2, PORT_2), "3" : (ADDR_3, PORT_3)}
reverse_rep_dict = {(ADDR_1, PORT_1) : "1", (ADDR_2, PORT_2) : "2", (ADDR_3, PORT_3) : "3"}

# replica connections, that are established, changed to the connection once connected
replica_connections = {"1" : 0, "2" : 0, "3" : 0}

replica_lock = Lock()  # lock on replica_connections


# message queues per username
message_queue = {}

# defined global variable of whether replica is primary or backup
global is_Primary
is_Primary = False

# caches for db operations
msgcache = {}
usercache = {}

# locks
dict_lock = Lock()  # client dict
msg_cache_lock = Lock()
user_cache_lock = Lock()


# DB OPERATIONS



USERFILEPATH = "user.json"
MSGFILEPATH = "sent.json"
MSGQPATH = "msg_queue.json"

msg_db = {}

with open(USERFILEPATH, 'w') as f:
    f.write(client_dictionary)
    f.close()

with open(MSGQPATH, 'w') as f:
    f.write(message_queue)
    f.close()

with open(MSGFILEPATH, 'w') as f:
    f.write(msg_db)
    f.close()



def load_db_to_state(path):
    try:
        with open(path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            client_dictionary = {}
            for row in csv_reader:
                key = row['key_column_name']
                client_dictionary[key] = row
    except FileNotFoundError:
        print("user db not found")
    return client_dictionary


def dump_cache(path, cache):
    try:
        with open(path, 'a', newline='') as file:
            # Define the fieldnames for the CSV
            fieldnames = cache.keys()

            # Create the DictWriter object
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Write the header row if the file is empty
            if file.tell() == 0:
                writer.writeheader()

            # Iterate over the dictionary of dictionaries
            for key1, inner_dict in cache.items():
                for row in inner_dict.values():
                    # Add the key1 to the row dictionary
                    row['key1'] = key1

                    # Write the row to the CSV file
                    writer.writerow(row)
            cache = {}
    except FileNotFoundError:
        print("db not found")


def add_to_cache(item, cache, lock):
    lock.acquire(timeout=10)
    cache.add(item)
    lock.release()


# for backup servers, updates server state as if it were interacting with the client, but without sending
def handle_message(message, tag=None):
    tag = message[0]
    # Wire protocol for replica interaction is different; need to read out the username relevant to the state change first
    length_of_username = message[1]
    username = message[2:2+length_of_username].decode()

    if tag == 0:
        # acquire lock for client_dictionary, with timeout in case of failure
        dict_lock.acquire(timeout=10)
        if username in client_dictionary.keys():
            pass
        else:
            # backup stores username as logged off, as we will automatically log off clients when the server crashes
            client_dictionary[username] = 0
            message_queue[username] = []
        dict_lock.release()

    if tag == 1:
        # we pass here, because a login doesn't change necessary state of the backup server, as the client will have to log in if the primary fails
        pass
    if tag == 2:
        # similar to above, logouts do not affect backup state
        pass
    if tag == 3:
        # deletes the username from backup server state
        dict_lock.acquire(timeout=10)
        client_dictionary.pop(username)
        message_queue.pop(username)
        dict_lock.release()
    if tag == 4:
        # sending messages

        length_of_recep = message[2+length_of_username]  # convert to int
        recep_username = message[3+length_of_username:3+length_of_username+length_of_recep].decode()

        dict_lock.acquire(timeout=10)
        # Checks if recipeint is actually a possible recipient
        if recep_username not in client_dictionary.keys():
            pass
        else:
            queue_tag = message[3+length_of_username+length_of_recep]
            text_message = message[4+length_of_username +
                                   length_of_recep:].decode()
            # Checks if recipient logged out
            if queue_tag == 0:
                message_queue[recep_username].append(
                    [username, text_message])

            # If logged in, look up connection in dictionary
            else:
                # TO DO: Persistent store the successfully sent message
                pass
        dict_lock.release()

    if tag == 5:
        # notification that message queue has been read from
        dict_lock.acquire(timeout=10)
        # TO DO: for message in message_queue, persistent store the successfully sent message

        # current active message_queue is empty in backup state
        message_queue[username] = []
        # TO DO: overwrite the persistent store state of the new current message_queue
        dict_lock.release()

    if tag == 6:
        # no state change for a lookup request
        pass

def send_to_replicas(message):
    for idx in replica_connections.keys():
        if idx != machine_idx:
            try:
                replica_connections[idx].sendall(message)
            except Exception as e:
                print(e)
                continue



# To Do: send connections the messages as the client sends them in, with adjusted wire protocol
def clientthread(conn, addr):

    client_state = True
    logged_in = False
    username = None
    while client_state:

        # maintain a state variable as logged in or logged off
        # while logged off, logged_in = False
        print("here again")
        # sends a message to the client whose user object is conn
        message = "Welcome to Messenger! Please login or create an account:"
        return_tag = (0).to_bytes(1, "big")
        bmsg = return_tag + message.encode()
        conn.sendall(bmsg)

        # client can only create an account or login while client state is False
        # To Do: big endian byte interpretation
        while logged_in == False:
            try:
                message = conn.recv(2048)
                # Send to remaining replica servers

                # check if message is of type create account or login
                # wire protocol demands initial byte is either 0 (create) or 1 (login) here
                # tag = int.from_bytes(message[0], "big")
                tag = message[0]

                # account creation
                if tag == 0:
                    print('create')
                    username = message[1:]
                    username = username.decode()
                    # If the username is in existence, server asks to retry.

                    # acquire lock for client_dictionary, with timeout in case of failure
                    dict_lock.acquire(timeout=10)

                    if username in client_dictionary.keys():
                        message = "The account " + username + " already exists. Please try again."
                        return_tag = (0).to_bytes(1, "big")
                        bmsg = return_tag + message.encode()
                        conn.sendall(bmsg)
                    else:
                        client_dictionary[username] = conn
                        message_queue[username] = []

                        # send update to replicas
                        update = (0).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode()
                        replica_lock.acquire()
                        send_to_replicas(update)
                        replica_lock.release()

                        # response to client
                        message = "Account created. Welcome " + username + "!"
                        return_tag = (1).to_bytes(1, "big")
                        bmsg = return_tag + message.encode()
                        conn.sendall(bmsg)
                        logged_in = True

                    dict_lock.release()

                # login
                if tag == 1:
                    username = message[1:]
                    username = username.decode()

                    # acquire lock for client_dictionary, with timeout in case of failure
                    dict_lock.acquire(timeout=10)

                    if username not in client_dictionary.keys():
                        message = "Username not found. Please try again."
                        return_tag = (0).to_bytes(1, "big")
                        bmsg = return_tag + message.encode()
                        conn.sendall(bmsg)
                    else:
                        # Check if username logged in elsewhere (i.e. dictionary returns 1)
                        if client_dictionary[username] != 0:
                            message = "Username logged in elsewhere. Please try again."
                            return_tag = (0).to_bytes(1, "big")
                            bmsg = return_tag + message.encode()
                            conn.sendall(bmsg)
                        else:
                            client_dictionary[username] = conn
                            message = "Welcome back " + username + "!"
                            return_tag = (1).to_bytes(1, "big")
                            bmsg = return_tag + message.encode()
                            conn.sendall(bmsg)
                            logged_in = True
                    print(client_dictionary)
                    dict_lock.release()

                # List Accounts if logged out
                if tag == 6:
                    query = message[1:].decode()
                    dict_lock.acquire(timeout=10)
                    users = match(query)
                    dict_lock.release()
                    if users == '':
                        res = 'No users found'
                    else:
                        res = "Users matching " + query + ':\n'
                        res += users

                    return_tag = (2).to_bytes(1, "big")
                    bmsg = return_tag + res.encode()
                    conn.sendall(bmsg)
            except:
                continue

        # now suppose that the client is logged in
        # allowable actions are: list accounts, send message, log off, delete account, dump queue

        while logged_in == True:
            try:
                message = conn.recv(2048)
                if message:
                    tag = message[0]

                    # Logout
                    if tag == 2:
                        # Acquire dict lock, change logged in state to false and remove address info in client dictionary
                        logout(username)
                        message = username + " successfully logged out. \n"
                        return_tag = (0).to_bytes(1, "big")
                        bmsg = return_tag + message.encode()
                        conn.sendall(bmsg)
                        logged_in = False

                    # Delete Account
                    if tag == 3:  # start of delete account

                        # Acquire dict lock, remove username from client dictionary and message_queue
                        dict_lock.acquire(timeout=10)
                        client_dictionary.pop(username)
                        message_queue.pop(username)
                        logged_in = False
                        dict_lock.release()

                        # update replicas
                        update = (3).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode()
                        replica_lock.acquire()
                        send_to_replicas(update)
                        replica_lock.release()

                        # update clients
                        message = "Account " + username + " successfully deleted. \n"
                        return_tag = (0).to_bytes(1, "big")
                        bmsg = return_tag + message.encode()
                        conn.sendall(bmsg)

                    # Send Message
                    if tag == 4:
                        # Wire Protocol: tag-length of username (< 256 char by demand) - recepient - message
                        length_of_recep = message[1]  # convert to int
                        recep_username = message[2:2+length_of_recep].decode()

                        dict_lock.acquire(timeout=10)
                        # To Do: Should we make this more granular with locking? Like variables for recep in client_dictionary
                        # Checks if recipeint is actually a possible recipient
                        if recep_username not in client_dictionary.keys():
                            message = "Sorry, message recipient not found. Please try again. \n"
                            return_tag = (1).to_bytes(1, "big")
                            bmsg = return_tag + message.encode()
                            conn.sendall(bmsg)
                        else:
                            text_message = message[2+length_of_recep:].decode()
                            # Checks if recipient logged out
                            if client_dictionary[recep_username] == 0:
                                message_queue[recep_username].append(
                                    [username, text_message])
                                
                                # update replicas
                                update = (4).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode() + message[1:2+length_of_recep] + (0).to_bytes(1, "big") + text_message.encode()
                                replica_lock.acquire()
                                send_to_replicas(update)
                                replica_lock.release()



                                confirmation_message = "\nMessage successfully sent."
                                return_tag = (1).to_bytes(1, "big")
                                bmsg = return_tag + confirmation_message.encode()
                                conn.sendall(bmsg)
                            # If logged in, look up connection in dictionary
                            else:
                                recep_conn = client_dictionary[recep_username]
                                new_message = "<"+username+">: " + text_message
                                try:

                                    # To Do: persistent store this

                                    # update replicas
                                    update = (4).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode() + message[1:2+length_of_recep] + (1).to_bytes(1, "big") + text_message.encode()
                                    replica_lock.acquire()
                                    send_to_replicas(update)
                                    replica_lock.release()


                                    return_tag = (1).to_bytes(1, "big")
                                    bmsg = return_tag + new_message.encode()
                                    recep_conn.sendall(bmsg)
                                    confirmation_message = "\nMessage successfully sent."
                                    bmsg = return_tag + confirmation_message.encode()
                                    conn.sendall(bmsg)
                                # If sending fails, let the sender know; otherwise, send confirmation to sender
                                except:
                                    error_message = "Sorry, message could not be sent. Please try again."
                                    return_tag = (1).to_bytes(1, "big")
                                    bmsg = return_tag + error_message.encode()
                                    conn.sendall(bmsg)

                        dict_lock.release()

                    # Dump Message queue
                    if tag == 5:
                        # Message Queue preconfigured so it stores packet of username, message
                        for undelivered in message_queue[username]:
                            sender = undelivered[0]
                            undel_message = undelivered[1]
                            message = "<" + sender + ">: " + undel_message
                            return_tag = (1).to_bytes(1, "big")
                            bmsg = return_tag + message.encode()
                            conn.sendall(bmsg)

                        # empties the message queue
                        message_queue[username] = []

                        # update replicas
                        update = (5).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode()
                        replica_lock.acquire()
                        send_to_replicas(update)
                        replica_lock.release()

                    if tag == 6:
                        query = message[1:].decode()
                        dict_lock.acquire(timeout=10)
                        users = match(query)
                        dict_lock.release()
                        if users == '':
                            res = 'No users found'
                        else:
                            res = "Users matching " + query + ':\n'
                            res += users

                        return_tag = (2).to_bytes(1, "big")
                        bmsg = return_tag + res.encode()
                        conn.sendall(bmsg)

                else:
                    """message may have no content if the connection
                    is broken, in this case we remove the connection"""
                    removeconn(conn, username)
                    logged_in = False
                    client_state = False

            # except Exception as e:
            except:
                continue


def logout(username):
    dict_lock.acquire(timeout=10)
    client_dictionary[username] = 0
    dict_lock.release()


def removeconn(connection, username):
    # If connection breaks, automatically logs username out
    logout(username)
    if connection in list_of_clients:
        print(f"{connection} has left")
        list_of_clients.remove(connection)


def match(query):
    message = ''
    try:
        for key in client_dictionary.keys():
            match = re.search(query, key)
            if match is not None and match.group() == key:
                message += key + " "
    except Exception as e:
        print(e)
        message = 'Regex Error'

    return message


# catch up on logs, and determine primary by connections
# init process:
prim_conn = None
# backups = [('ip1', 0), ('ip2', 1), ('ip3', 2)]  # change to actual IP
files_to_expect = [USERFILEPATH, MSGFILEPATH, MSGQPATH]
local_to_load = [client_dictionary, None, message_queue]



server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((IP, port))
server.listen()

client_dictionary = load_db_to_state(USERFILEPATH)





# initialization
# only while a server is_Primary=True can it accept connections 
primary_exists = False
for idx in replica_dictionary.keys():
    if idx != machine_idx:
        try:
            conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_socket.connect((replica_dictionary[idx]))

            # store connection in replica_connections
            replica_lock.acquire()
            replica_connections[idx] = conn_socket
            replica_lock.release()

            # received tag from other replicas, 0 implies backup, 1 implies primary
            tag = conn_socket.recv(2048)
            if tag == 1:
                primary_exists = True
                prim_conn = conn_socket

                try:
                    for i in range(len(files_to_expect)):
                        id = server.recv(4)
                        id = int.from_bytes(file_size, byteorder='big')
                        file_size = server.recv(8)
                        file_size = int.from_bytes(file_size, byteorder='big')
                        byteswritten = 0
                        with open(f'{files_to_expect[id]}', 'wb') as f:
                            # receive the file contents
                            while byteswritten < file_size:
                                data = server.recv(1024)
                                f.write(data)
                                byteswritten += len(data)
                        if local_to_load is not None and byteswritten != 0:
                            load_db_to_state(files_to_expect[i])
                except Exception as e:
                    print('init error', e)


            if tag == 0:
                # reached out to backup, so nothing to change here, other than replica connection
                pass
                
        except ConnectionRefusedError:
            pass
        except Exception as e:
            print(e)

# if no primary exists, default primary
if primary_exists == False:
    is_Primary = True


def backup_connections():
    while is_Primary == False:
        conn, addr = server.accept()
        print(addr[0] + " connected")
        if addr in replica_dictionary.values():
            replica_lock.acquire()
            replica_connections[reverse_rep_dict[addr]] = conn
            replica_lock.release()
            bmsg = (0).to_bytes(1, "big")
            conn.sendall(bmsg)

def backup_message_handling():
    while is_Primary == False:
        try:
            # FIX THIS for message queue
            msg = prim_conn.recv(2048)
            if msg:
                handle_message(msg)
            else:
                # make current cache persistent
                dump_cache(MSGFILEPATH, msgcache)

                # handle leader election
                # if this doesnt work, use test sockets that are closed
                is_Lowest = True
                for i in range(1, int(machine_idx)):
                    try:
                        replica_lock.acquire()
                        replica_connections[str(i)].sendall(b'hb')
                        replica_lock.release()
                        is_Lowest = False
                    except (ConnectionResetError, BrokenPipeError):
                        pass
                    except Exception as e:
                        print(e)
                if is_Lowest == True:
                    is_Primary = True

        except Exception as e:
            print(e)
            continue



while True:
    # backup server loop
    start_new_thread(backup_connections, ())
    start_new_thread(backup_message_handling, ())
    # while is_Primary == False:
    #     try:
    #         msg = prim_conn.recv(2048)
    #         if msg:
    #             handle_message(msg)
    #             # message_queue.add(msg)
    #             # prim_conn.send(1)
    #             # sent = prim_conn.recv(2048)
    #             # if sent == 1:
    #             #     msgcache.add(msg)
    #                 # if len(msgcache) >= 10:
    #                 #     dump_cache(MSGFILEPATH, msgcache)
    #         else:
    #             # server broken, find next leader




               

        # except Exception as e:
        #     print(e)
        #     continue

    while is_Primary == True:
        # running client thread on primary server
        conn, addr = server.accept()
        print(addr[0] + " connected")
        # is a reconnecting replica:
        if addr in replica_dictionary.values():
            replica_lock.acquire()
            replica_connections[reverse_rep_dict[addr]] = conn
            replica_lock.release()
            # sends tag that this connection is the primary
            bmsg = (1).to_bytes(1, "big")
            conn.sendall(bmsg)

            # sends logs of client dict, sent messages, and message queue
            # with open(USERFILEPATH, "rb") as f:
            #     usr_file_contents = f.read()
            # tag = (1).to_bytes(1, "big")
            # usr_file_contents = tag + usr_file_contents
            # conn.sendall(usr_file_contents)
            
            # with open(MSGFILEPATH, "rb") as f:
            #     msg_file_contents = f.read()
            # tag = (2).to_bytes(1, "big")
            # msg_file_contents = tag + msg_file_contents
            # conn.sendall(msg_file_contents)

            for i in range(len(files_to_expect)):
                file = files_to_expect[i]
                sendafile = open(file, "rb")
                filesize = os.path.getsize(file)
                id = (i).to_bytes(4, "big")
                size = (filesize).to_bytes(8, "big")
                conn.sendall(id)
                conn.sendall(size)
                try:
                    # Send the file over the connection
                    conn.sendfile(sendafile)
                    sendafile.close()
                except:
                    print('file error')            
        else:
            list_of_clients.append(conn)
            # creates an individual thread for each machine that connects
            start_new_thread(clientthread, (conn, addr))







# does the primary need multiple threads to hear the confirmation from each thread separately?






# if backups[0][0] == IP and backups[0][1] == port:
#     is_Primary = True
# else:
#     try:
#         res = server.connect(backups[0][0], backups[0][1])
#         prim_conn = res
#     except:
#         print('init error')

# backups.pop(0)

# need to clarify the process of rejoining
 # if backups[0][0] == IP and backups[0][1] == port:
                #     backups.pop(0)
                #     is_Primary = True
                #     prim_conn = None
