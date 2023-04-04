# Python program to implement client side of chat room.
import socket
import select
import sys

MAX_MESSAGE_LENGTH = 280
MAX_RECIPIENT_LENGTH = 50

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
if len(sys.argv) != 3:
    print("Correct usage: script, IP address, port number")
    exit()
IP_address = str(sys.argv[1])
Port = int(sys.argv[2])
server.connect((IP_address, Port))


# Keywords that client side parses and tags to send to the server
MESSAGE_KEYS = ['Create Account', 'Login', 'Logout',
                'Delete Account', 'Send', 'List Accounts']



# always returns encoded message
def process(message, client_logged_in):
    message = message.rstrip()
    # Messages are first entered as types, and are tagged based on those types

    # CREATE ACCOUNT
    if message.find('Create Account') == 0:
        #Can't create an account once logged in
        if client_logged_in == True:
            print("Please logout to create an account.")
            return
        name = ""
        name_max = False
        # Ensures created accounts are no more than the max alotted length
        while name_max == False:
            name = sys.stdin.readline()
            name = name.rstrip()
            if len(name) <= MAX_RECIPIENT_LENGTH:
                name_max = True
            else:
                print("All usernames must be at most " +
                    str(MAX_RECIPIENT_LENGTH) + " characters. Please try again.")
        message = name
        message = message.encode()
        tag = (0).to_bytes(1, "big")
    
    # LOGIN
    elif message.find('Login') == 0:
        # Can't login if already logged in
        if client_logged_in == True:
            print("Please logout to login.")
            return
        message = sys.stdin.readline()
        message = message.rstrip()
        message = message.encode()
        tag = (1).to_bytes(1, "big")

    # LOGOUT
    elif message.find('Logout') == 0:
        #Can't log out if already logged out
        if client_logged_in == False:
            print("Currently logged out. Please create an account or login.")
            return
        message = ""
        message = message.encode()
        tag = (2).to_bytes(1, "big")

    # DELETE ACCOUNT
    elif message.find('Delete Account') == 0:
        # Can't delete account unless logged in
        if client_logged_in == False:
            print("Currently logged out. Please create an account or login.")
            return
        message = ""
        message = message.encode()
        tag = (3).to_bytes(1, "big")

    #SEND
    elif message.find("Send") == 0:
        # Can only send if logged in
        if client_logged_in == False:
            print("Currently logged out. Please create an account or login.")
            return
        print("To: ")
        recipient = ""
        rec_max = False
        # Ensures recipient is in the length limit, for wire protocol tagging
        while rec_max == False:
            recipient = sys.stdin.readline()
            recipient = recipient.rstrip()
            if len(recipient) <= MAX_RECIPIENT_LENGTH:
                rec_max = True
            else:
                print("All usernames are at most " +
                      str(MAX_RECIPIENT_LENGTH) + " characters. Please try again.")
        len_r = len(recipient)
        recep_tag = (len_r).to_bytes(1, "big")
        recipient = recipient.encode()
        print("Message: ")
        mes_len = False
        message = ""
        # Ensures message is in the length limit
        while mes_len == False:
            message = sys.stdin.readline()
            message = message.rstrip()
            if len(message) <= MAX_MESSAGE_LENGTH:
                mes_len = True
            else:
                print("Message must be at most " +
                      str(MAX_MESSAGE_LENGTH) + " characters. Please try again.")
        message = message.encode()
        type_tag = (4).to_bytes(1, "big")
        # Wire protocol tags with type, then length of receiving username for metadata parsing, and the receiving username
        tag = type_tag + recep_tag + recipient

    # OPEN UNDELIVERED MESSAGES
    elif message.find("Open Undelivered Messages") == 0:
        if client_logged_in == False:
            print("Currently logged out. Please create an account or login.")
            return
        message = ""
        message = message.encode()
        tag = (5).to_bytes(1, "big")

    # LIST ACCOUNTS
    elif message.find("List Accounts") == 0:
        message = sys.stdin.readline()
        message = message.rstrip()
        message = message.encode()
        tag = (6).to_bytes(1, "big")

    # UNRECOGNIZED INPUT
    else:
        print('Input not recognized. Please try again.')
        return

    bmsg = tag + message
    return bmsg

# global state of logged in that changes according to server sending updates on login/logout status
global client_logged_in
client_logged_in = False

while True:

    # maintains a list of possible input streams
    sockets_list = [sys.stdin, server]

    """ There are two possible input situations. Either the
    user wants to give manual input to send to other people,
    or the server is sending a message to be printed on the
    screen. Select returns from sockets_list, the stream that
    is reader for input. So for example, if the server wants
    to send a message, then the if condition will hold true
    below.If the user wants to send a message, the else
    condition will evaluate as true"""
    read_sockets, write_socket, error_socket = select.select(
        sockets_list, [], [])
    

    for socks in read_sockets:
        if socks == server:
            message = socks.recv(2048)
            tag = message[0]
            if tag == 0:
                client_logged_in = False
            elif tag == 1:
                client_logged_in = True
            # Tag 2 corresponds to list accounts, which doesnt require account state logged in or off, so state doesn't change here
            else:
                pass
            dmessage = message[1:].decode()
            print(dmessage)
        else:
            message = sys.stdin.readline()
            bmsg = process(message, client_logged_in)
            if bmsg:
                try:
                    server.send(bmsg)
                    sys.stdout.flush()
                except:
                    print('Message could not send.')
server.close()


