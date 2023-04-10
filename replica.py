import time
import socket
import traceback
import sys
from _thread import *
from threading import Lock
import re
import json
import os
import json
import select
import threading

NUM_MACHINES = 3
ADDR_1 = "10.250.11.249"
ADDR_2 = "10.250.11.249"
ADDR_3 = "10.250.11.249"

PORT_1 = 9080
PORT_2 = 9081
PORT_3 = 9082

CPORT_1 = 8080
CPORT_2 = 8081
CPORT_3 = 8082


ADDRS = [ADDR_1, ADDR_2, ADDR_3]
PORTS = [PORT_1, PORT_2, PORT_3]
CPORTS = [CPORT_1, CPORT_2, CPORT_3]

#sudo kill -9 $(sudo lsof -t -i :8080)


# Machine number
machine_idx = str(sys.argv[1])


# IP address 
IP = ADDRS[int(machine_idx)-1]


# Server Port number
s_port = PORTS[int(machine_idx)-1]

# Client Port number
c_port = CPORTS[int(machine_idx)-1]



# maintains a list of potential clients
list_of_clients = []

# client username dictionary, with login status: 0 if logged off, corresponding address if logged in
client_dictionary = {}
user_state_dictionary = {}


# replica dictionary, keyed by address and valued at machine id
replica_dictionary = {"1": (ADDR_1, PORT_1), "2": (
    ADDR_2, PORT_2), "3": (ADDR_3, PORT_3)}
reverse_rep_dict = {(ADDR_1, PORT_1): "1", (ADDR_2, PORT_2): "2", (ADDR_3, PORT_3): "3"}

# replica connections, that are established, changed to the connection once connected
replica_connections = {"1": 0, "2": 0, "3": 0}

replica_lock = Lock()  # lock on replica_connections


# message queues per username
message_queue = {}
msg_db = {}

# defined global variable of whether replica is primary or backup
# global is_Primary
is_Primary = False

# locks
dict_lock = Lock()  # client dict
msg_cache_lock = Lock()
user_cache_lock = Lock()


# DB OPERATIONS


USERFILEPATH = "user" + machine_idx + ".json"
MSGFILEPATH = "sent" + machine_idx + ".json"
MSGQPATH = "msg_queue" + machine_idx + ".json"

msg_db = {}

with open(USERFILEPATH, 'w') as f:
    json_data = json.dumps(user_state_dictionary)
    f.write(json_data)
    f.close()

with open(MSGQPATH, 'w') as f:
    json_data = json.dumps(message_queue)
    f.write(json_data)
    f.close()

with open(MSGFILEPATH, 'w') as f:
    json_data = json.dumps(msg_db)
    f.write(json_data)
    f.close()


def load_db_to_state(path):
    try:
        res_dictionary = json.load(path)
    except:
        res_dictionary = {}
    return res_dictionary


# def dump_cache(path, cache):
#     try:
#         with open(path, 'a', newline='') as file:
#             # Define the fieldnames for the CSV
#             fieldnames = cache.keys()

#             # Create the DictWriter object
#             writer = csv.DictWriter(file, fieldnames=fieldnames)

#             # Write the header row if the file is empty
#             if file.tell() == 0:
#                 writer.writeheader()

#             # Iterate over the dictionary of dictionaries
#             for key1, inner_dict in cache.items():
#                 for row in inner_dict.values():
#                     # Add the key1 to the row dictionary
#                     row['key1'] = key1

#                     # Write the row to the CSV file
#                     writer.writerow(row)
#             cache = {}
#     except FileNotFoundError:
#         print("db not found")


def write(flag):
    if flag == 0:
        dict = user_state_dictionary
    elif flag == 1:
        dict = msg_db
    elif flag == 2:
        dict = message_queue
    file = files_to_expect[flag]
    try:
        print(dict, file)
        with open(file, 'w') as f:
            json.dump(dict, f)
    except:
        print(f'ERROR: could not write to {file}')


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
            user_state_dictionary[username] = 0
            message_queue[username] = []
            write(0)
            write(2)
        dict_lock.release()

    if tag == 3:
        # deletes the username from backup server state
        dict_lock.acquire(timeout=10)
        client_dictionary.pop(username)
        user_state_dictionary.pop(username)
        message_queue.pop(username)
        write(0)
        dict_lock.release()

    if tag == 4:
        # adding messages to queue

        length_of_recep = message[2+length_of_username]  # convert to int
        recep_username = message[3+length_of_username:3 +
                                 length_of_username+length_of_recep].decode()

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
                write(2)
        dict_lock.release()

    if tag == 5:
        # notification that message queue has been read from
        dict_lock.acquire(timeout=10)
        # TO DO: for message in message_queue, persistent store the successfully sent message

        # current active message_queue is empty in backup state
        msg_db[username].append(message_queue[username])
        message_queue[username] = 0
        # TO DO: overwrite the persistent store state of the new current message_queue
        write(1)
        write(2)
        dict_lock.release()




def send_to_replicas(message):
    for idx in replica_connections.keys():
        if idx != machine_idx and replica_connections[idx] != 0:
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
                        user_state_dictionary[username] = 1
                        message_queue[username] = []

                        # send update to replicas
                        update = (0).to_bytes(1, "big") + (len(username)
                                                           ).to_bytes(1, "big") + username.encode()
                        replica_lock.acquire()
                        print(client_dictionary)
                        send_to_replicas(update)
                        write(0)
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
                            user_state_dictionary[username] = conn
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
                        user_state_dictionary.pop(username)
                        message_queue.pop(username)
                        logged_in = False
                        dict_lock.release()

                        # update replicas
                        update = (3).to_bytes(1, "big") + (len(username)
                                                           ).to_bytes(1, "big") + username.encode()
                        replica_lock.acquire()
                        send_to_replicas(update)
                        write(0)
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
                                update = (4).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode(
                                ) + message[1:2+length_of_recep] + (0).to_bytes(1, "big") + text_message.encode()
                                replica_lock.acquire()
                                send_to_replicas(update)
                                write(2)
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
                                    update = (4).to_bytes(1, "big") + (len(username)).to_bytes(1, "big") + username.encode(
                                    ) + message[1:2+length_of_recep] + (1).to_bytes(1, "big") + text_message.encode()
                                    replica_lock.acquire()
                                    send_to_replicas(update)
                                    write(2)
                                    write(1)
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
                        update = (5).to_bytes(1, "big") + (len(username)
                                                           ).to_bytes(1, "big") + username.encode()
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
    user_state_dictionary[username] = 0
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
            user_state_dictionary[username] = 0
            message_queue[username] = []
            # update backups
            write(0)
            write(2)

        dict_lock.release()

    if tag == 3:
        # deletes the username from backup server state
        dict_lock.acquire(timeout=10)
        client_dictionary.pop(username)
        user_state_dictionary.pop(username)
        message_queue.pop(username)
        dict_lock.release()
        # persist deletion of user
        write(0)
    if tag == 4:
        # adding messages that have not been sent to the queue

        length_of_recep = message[2+length_of_username]  # convert to int
        recep_username = message[3+length_of_username:3 +
                                 length_of_username+length_of_recep].decode()

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
                write(2)
        dict_lock.release()

    if tag == 5:
        # notification that message queue has been read from
        dict_lock.acquire(timeout=10)
        # TO DO: for message in message_queue, persistent store the successfully sent message

        # current active message_queue is empty in backup state
        message_queue[username] = 0
        write(2)
        dict_lock.release()





def backup_message_handling():
    global is_Primary
    global prim_conn
    while is_Primary == False:     
        msg = prim_conn.recv(2048)
        if msg:
            handle_message(msg)
        else:
            for i in range(len(files_to_expect)):
                write(i)

                # handle leader election
                # if this doesnt work, use test sockets that are closed
                # THIS DOES NOT WORK, REPLICA_CONNECTIONS IS NOT BEING UPDATED
            is_Lowest = True
            for i in range(1, int(machine_idx)):
                try:
                    # time.sleep((int(machine_idx)-1)*2)
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.connect((ADDRS[i-1], PORTS[i-1]))
                    # test_socket.settimeout(int(machine_idx))
                    test_socket.sendall(int(machine_idx).to_bytes(1, "big"))

                    test_socket.settimeout(None)
                    replica_lock.acquire()
                    if replica_connections[str(i)] != 0:
                        replica_connections[str(i)].close()
                    replica_connections[str(i)] = test_socket
                    replica_lock.release()

                    ret_tag = test_socket.recv(1)[0]
                    # print(ret_tag)
                    # print("HUH")
                    # if ret_tag == 1:
                        # print(ret_tag)
                    is_Lowest = False
                    prim_conn = replica_connections[str(i)]
                    # print(ret_tag)

                    
                except ConnectionRefusedError:
                    replica_lock.acquire()
                    if replica_connections[str(i)] != 0:
                        replica_connections[str(i)].close()
                    replica_connections[str(i)] = 0
                    replica_lock.release()
                    continue
                except Exception as e:
                    print("B")
                    print(i)
                    print("-  -  -  -")
                    print(e)
                    replica_lock.acquire()
                    if replica_connections[str(i)] != 0:
                        replica_connections[str(i)].close()
                    replica_connections[str(i)] = 0
                    replica_lock.release()
                    continue
            if is_Lowest == True:
                is_Primary = True
            print("election done")
            print(is_Primary)
        


def server_interactions():
    global is_Primary
    while True:
        conn, addr = backupserver.accept()
        if is_Primary == False:
            # backup behavior
            # tells other incoming connections that it is a backup replica
            conn_type = conn.recv(1)
            index_of_connector = conn_type[0]
            print(index_of_connector)
            print("backup reception")
            key = str(index_of_connector)
            # is a reconnecting replica:
            if key in replica_dictionary.keys():
                replica_lock.acquire()
                #THIS DOES NOT DISTINGUISH
                replica_connections[key] = conn
                replica_lock.release()
                bmsg = (0).to_bytes(1, "big")
                conn.sendall(bmsg)
        else:
            # primary behavior
            conn_type = conn.recv(1)
            index_of_connector = conn_type[0]
            print(index_of_connector)
            print("primary reception")
            key = str(index_of_connector)
            if key in replica_dictionary.keys():
                replica_lock.acquire()
                replica_connections[key] = conn
                replica_lock.release()
                # sends tag that this connection is the primary
                bmsg = (1).to_bytes(1, "big")
                conn.sendall(bmsg)

                # sends logs of client dict, sent messages, and message queue, for catchup
                for i in range(len(files_to_expect)):
                    file = files_to_expect[i]
                    filesize = os.path.getsize(file)
                    id = (i).to_bytes(4, "big")
                    size = (filesize).to_bytes(8, "big")
                    conn.sendall(id)
                    conn.sendall(size)
                    try:
                        with open(file, 'rb') as sendafile:
                            # Send the file over the connection
                            bytesread = sendafile.read(1024)
                            if not bytesread:
                                break
                            conn.sendall(bytesread)
                    except:
                        print('file error')

def client_interactions():
    while True:
        # only handles clientside if it is currently the primary
        if is_Primary == True:
            print("entering client")
            conn, addr = clientserver.accept()
            list_of_clients.append(conn)
            # creates an individual thread for each machine that connects
            start_new_thread(clientthread, (conn, addr))



# FULL INITIALIZATION
# catch up on logs, and determine primary by connections
# init process:
global prim_conn
prim_conn = None
# backups = [('ip1', 0), ('ip2', 1), ('ip3', 2)]  # change to actual IP
files_to_expect = [USERFILEPATH, MSGFILEPATH, MSGQPATH]
local_to_load = [user_state_dictionary, msg_db, message_queue]


backupserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
backupserver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
backupserver.bind((IP, s_port))
backupserver.listen()

inputs = [backupserver]

# HANDLES CLIENT SIDE
clientserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientserver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
clientserver.bind((IP, c_port))
clientserver.listen()
inputs.append(clientserver)
for i in range(len(local_to_load)):
    local_to_load[i] = load_db_to_state(
        files_to_expect[i])  # persistence for the primary


# reaching out
# only while a server is_Primary=True can it accept connections
primary_exists = False
for idx in replica_dictionary.keys():
    # print(replica_dictionary, idx)
    if idx != machine_idx:
        try:
            conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # print(replica_dictionary[idx])
            conn_socket.connect(replica_dictionary[idx])
            mtag = int(machine_idx).to_bytes(1, "big")
            conn_socket.sendall(mtag)

            # store connection in replica_connections
            replica_lock.acquire()
            # ONLY GETS HERE ONCE FOR SECOND SERVER
            replica_connections[idx] = conn_socket
            replica_lock.release()

            # received tag from other replicas, 0 implies backup, 1 implies primary
            tag = conn_socket.recv(1)
            if tag[0] == 1:
                primary_exists = True
                prim_conn = conn_socket
                try:
                    for i in range(len(files_to_expect)):
                        id = conn_socket.recv(4)
                        id = int.from_bytes(id, byteorder='big')
                        file_size = conn_socket.recv(8)
                        file_size = int.from_bytes(file_size, byteorder='big')
                        byteswritten = 0
                        with open(f'{files_to_expect[id]}', 'wb') as f:
                            # receive the file contents
                            while byteswritten < file_size:
                                buf = min(file_size - byteswritten, 1024)
                                data = conn_socket.recv(buf)
                                f.write(data)
                                byteswritten += len(data)
                        if local_to_load is not None and byteswritten != 0:
                            load_db_to_state(files_to_expect[i])
                except Exception as e:
                    print('init error', e)
                    traceback.print_exc()

            if tag == 0:
                # reached out to backup, so nothing to change here, other than replica connection
                pass

        except ConnectionRefusedError:
            pass
        except Exception as e:
            traceback.print_exc()
            print('hello')


# if no primary exists, default primary
if primary_exists == False:
    is_Primary = True

print(is_Primary)
    
# if is_Primary == False:
#     start_new_thread(backup_message_handling, ())

thread_list = []
(threading.Thread(target=backup_message_handling)).start()
(threading.Thread(target=server_interactions)).start()
(threading.Thread(target=client_interactions)).start()




            

# does the primary need multiple threads to hear the confirmation from each thread separately?
