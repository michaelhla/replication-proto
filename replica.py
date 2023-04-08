import socket
import select
import sys
from _thread import *
from threading import Lock
import re
import threading
import csv

NUM_MACHINES = 3
ADDR_1 = ""
ADDR_2 = ""
ADDR_3 = ""

# IP address is first argument
IP = str(sys.argv[1])


# Port number is second argument
port = int(sys.argv[2])

# maintains a list of potential clients
list_of_clients = []

# client username dictionary, with login status: 0 if logged off, corresponding address if logged in
client_dictionary = {}


# maintains list of active replicas
list_of_replicas = []


# replica dictionary, keyed by address and valued at machine id
replica_dictionary = {ADDR_1: 1, ADDR_2: 2, ADDR_3: 3}

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


USERFILEPATH = ""
MSGFILEPATH = ""


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
        recep_username = message[3+length_of_username:3 +
                                 length_of_username+length_of_recep].decode()

        dict_lock.acquire(timeout=10)
        # Checks if recipeint is actually a possible recipient
        if recep_username not in client_dictionary.keys():
            pass
        else:
            text_message = message[3+length_of_username +
                                   length_of_recep:].decode()
            # Checks if recipient logged out
            if client_dictionary[recep_username] == 0:
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
        message_queue[username] = 0
        # TO DO: overwrite the persistent store state of the new current message_queue
        dict_lock.release()

    if tag == 6:
        # no state change for a lookup request
        pass


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
                                confirmation_message = "\nMessage successfully sent."
                                return_tag = (1).to_bytes(1, "big")
                                bmsg = return_tag + confirmation_message.encode()
                                conn.sendall(bmsg)
                            # If logged in, look up connection in dictionary
                            else:
                                recep_conn = client_dictionary[recep_username]
                                new_message = "<"+username+">: " + text_message
                                try:
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
                            # To Do: username will have /n at the end
                            message = "<" + sender + ">: " + undel_message
                            return_tag = (1).to_bytes(1, "big")
                            bmsg = return_tag + message.encode()
                            conn.sendall(bmsg)

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
backups = [('ip1', 0), ('ip2', 1), ('ip3', 2)]  # change to actual IP


# hard coded initialization
# this does not account for rejoining

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((IP, port))
server.listen()

client_dictionary = load_db_to_state(USERFILEPATH)

if backups[0][0] == IP and backups[0][1] == port:
    is_Primary = True
else:
    try:
        res = server.connect(backups[0][0], backups[0][1])
        prim_conn = res
    except:
        print('init error')

backups.pop(0)

# need to clarify the process of rejoining

while True:
    # backup server loop
    while is_Primary == False:
        try:
            msg = prim_conn.recv(2048)
            if msg:
                message_queue.add(msg)
                prim_conn.send(1)
                sent = prim_conn.recv(2048)
                if sent == 1:
                    msgcache.add(msg)
                    if len(msgcache) >= 10:
                        dump_cache(MSGFILEPATH, msgcache)
            else:
                # server broken, find next leader
                if backups[0][0] == IP and backups[0][1] == port:
                    backups.pop(0)
                    is_Primary = True
                    prim_conn = None

        except Exception as e:
            print(e)
            continue

    while is_Primary == True:
        # running client thread on primary server
        conn, addr = server.accept()
        print(addr[0] + " connected")
        if conn in backups:

            # is a reconnecting replica:
            # redefine primary
        else:
            list_of_clients.append(conn)
            # creates an individual thread for each machine that connects
            start_new_thread(clientthread, (conn, addr))


# does the primary need multiple threads to hear the confirmation from each thread separately?
