import threading
import socket
import time
import csv
from collections import defaultdict
from utils import object_to_txt
from enum import Enum
import ctypes


class TransactionEnum(Enum):
    INTRA_SHARD = 1
    CROSS_SHARD = 2

class Transaction:

    def __init__(self, x, y, amount, type, client_id=None):
        self.x = x
        self.y = y
        self.amount = amount
        self.type = type
        if client_id == None:
            self.client_id = id(self)
        else:
            self.client_id = client_id
    
    def to_dict(self):
        return {
            'x': self.x,
            'y': self.y,
            'amount': self.amount,
            'type': self.type,
            'client_id': self.client_id
        }

    @classmethod
    def from_dict(cls, dict_obj):
        return cls(dict_obj['x'], dict_obj['y'], dict_obj['amount'], dict_obj['type'], dict_obj['client_id'])


def prepare_monitor(thread1, thread2, client1, client2, transaction, x, y, prepare_phase, ack_messages):

    while True:


        if len(prepare_phase) != 0:
            # print(prepare_phase)
            count = 0
            for p in prepare_phase:
                if p == "PREPARE_SUCCESS":
                    count += 1

                    if count == 2:
                        # send commit
                        transaction.y = 0
                        client1.send(bytes(f"SERVER_RELAY@{transaction.client_id}#COMMIT|{object_to_txt(transaction)}|@", "utf-8"))

                        transaction.y = y
                        transaction.x = 0
                        client2.send(bytes(f"SERVER_RELAY@{transaction.client_id}#COMMIT|{object_to_txt(transaction)}|@", "utf-8"))
                        flag = 1
                        break
                else:
                    print("prepare fail")
                    stop_and_send_abort(thread1, thread2, client1, client2, transaction, x, y)
                    prepare_phase.clear()
                    break

        
        if len(ack_messages) != 0:
            print(ack_messages)
            flag = 0
            count = 0
            neg_ack = 0
            for m in ack_messages:

                if m == "ACK_SUCCESS":
                    count += 1
                    if count == 2:
                    # send commit
                        print("Succeeded 2pc")
                        # transaction.y = 0
                        # client1.send(bytes(f"SERVER_RELAY@{transaction.client_id}#COMMIT|{object_to_txt(transaction)}|@", "utf-8"))

                        # transaction.x = 0
                        # client2.send(bytes(f"SERVER_RELAY@{transaction.client_id}#COMMIT|{object_to_txt(transaction)}|@", "utf-8"))
                        flag = 1
                        break
                else:
                    neg_ack += 1

                    if neg_ack == 2:
                        print("2pc aborted")
                    # stop_and_send_abort(thread1, thread2, client1, client2, transaction, x, y)
                        flag = 1 
                        break

            if flag == 1:
                break


def transaction_coordinator(client, prepare_phase, ack_messages):
    
    while True:
        message = client.recv(6000).decode("utf-8")

        # Have a timeout for this
        if message == "PREPARE_SUCCESS":
            reset_timer()
            if len(prepare_phase) == 0:
                prepare_phase.append("PREPARE_SUCCESS")
            
        elif message == "PREPARE_FAIL":
            stop_timer()
            if len(prepare_phase) == 0:
                prepare_phase.append("PREPARE_FAIL")
            # Abort transaction

        elif message == "COMMITED":
            print("Ack received")
            break

        elif message == "ACK_FAIL":
            ack_messages.append("ACK_FAIL")
            if len(ack_messages) == 2:
                break
            

        elif message == "ACK_SUCCESS":
            ack_messages.append("ACK_SUCCESS")
            if len(ack_messages) == 2:
                break



def stop_thread(thread):
    if not thread.is_alive():
        return
    thread_id = thread.ident
    print(f"Stopping thread {thread_id}")
    
    # Raise SystemExit inside the target thread
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread_id),
        ctypes.py_object(SystemExit)
    )
    if res > 1:
        # If more than one thread was affected, revert the action
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
        print("Failed to stop thread")

start_time = time.time()
is_timer_running = True

def stop_and_send_abort(thread1, thread2, client1, client2, transaction, x, y):
    # stop_thread(thread1)
    # stop_thread(thread2)

    print("sending abort")
    transaction.y = 0
    client1.send(bytes(f"SERVER_RELAY@{transaction.client_id}#ABORT|{object_to_txt(transaction)}|@", "utf-8"))

    transaction.y = y
    transaction.x = 0
    client2.send(bytes(f"SERVER_RELAY@{transaction.client_id}#ABORT|{object_to_txt(transaction)}|@", "utf-8"))

def stop_timer():
    global is_timer_running
    is_timer_running = False

def timer(thread1, thread2, client1, client2, transaction, x, y):

    time_limit = 5  # Timeout after 5 seconds
    global start_time
    start_time = time.time()

    global is_timer_running
    is_timer_running = True
    while is_timer_running:
        elapsed_time = time.time() - start_time
        if elapsed_time > time_limit:
            stop_and_send_abort(thread1, thread2, client1, client2, transaction, x, y)
            stop_timer()
            # self.reset_timer()
            print("Timeout occurred!")
        
        # Simulate some work
        print("Working...")
        time.sleep(5)


    pass

def reset_timer():
    global start_time
    start_time = time.time()


def handle(client):
    
    while True:
        message = client.recv(6000).decode("utf-8")
            # message, piggy_back_obj = message.split("|")

        if message == "COMMIT_SUCCESS":
            print(message)
            client.close()
            break
            
        elif message == "INSUFFICIENT_FUNDS":
            print(message)
            client.close()
            break
        elif message == "ABORT":
            print(message)
            print("Abort!")
            break
        # message_split = message.split("|")


def handle_cross_shard(x, y, amount, cluster_port1, cluster_port2):
    prepare_phase = []
    ack_messages = []
    transaction = Transaction(x, y, amount, 'cross_shard')
    print("Sent Message")
    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect(('localhost', cluster_port1))
    transaction.y = 0
    clientsocket1.send(bytes(f"CLIENT_INIT@{transaction.client_id}#PREPARE|{object_to_txt(transaction)}|@", "utf-8"))


    clientsocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket2.connect(('localhost', cluster_port2))
    transaction.y = y
    transaction.x = 0 
    clientsocket2.send(bytes(f"CLIENT_INIT@{transaction.client_id}#PREPARE|{object_to_txt(transaction)}|@", "utf-8"))

    thread1 = threading.Thread(target=transaction_coordinator, args=(clientsocket1, prepare_phase, ack_messages, ))
    thread1.start()

    thread2 = threading.Thread(target=transaction_coordinator, args=(clientsocket2, prepare_phase, ack_messages, ))
    thread2.start()




    # timer_thread = threading.Thread(target=timer, args=(thread1, thread2, clientsocket1, clientsocket2, transaction, x, y, ))
    # timer_thread.start()

    
    thread = threading.Thread(target=prepare_monitor, args=(thread1, thread2, clientsocket1, clientsocket2, transaction, x, y, prepare_phase, ack_messages, ))
    thread.start()

def handle_intra_shard(x, y, amount, cluster_port):
    transaction = Transaction(x, y, amount, 'intra_shard')
    print("Sent Message")
    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect(('localhost', cluster_port))
    clientsocket1.send(bytes(f"CLIENT_INIT@{transaction.client_id}#CLIENT|{object_to_txt(transaction)}|@", "utf-8"))

    thread = threading.Thread(target=handle, args=(clientsocket1, ))
    thread.start()

if __name__ == "__main__":

    # comm_manager: ClientManager = ClientManager()
    # comm_manager.receive()

    time.sleep(5)

    cluster1 = set(list(range(1, 1001)))
    cluster2 = set(list(range(1001, 2001)))
    cluster3 = set(list(range(2001, 3001)))

    cluster1_servers = [1, 2, 3]

    cluster1_port = 8080
    cluster2_port = 8081
    cluster3_port = 8082


    with open('transaction.csv', 'r') as file:
        csv_reader = csv.reader(file)
        # csv_reader.__next__()
        for row in csv_reader:
            x, y, amount = row
            x, y, amount = int(x), int(y), float(amount)

            if x == y:
                print("Self transaction is not possible!")
                continue

            if x in cluster1 and y in cluster1:
                # intrashard cluster1
                handle_intra_shard(x, y, amount, cluster1_port)
                # time.sleep(0.5)
                
            elif x in cluster2 and y in cluster2:
                # intrashard cluster2
                handle_intra_shard(x, y, amount, cluster2_port)

            elif x in cluster3 and y in cluster3:
                # intrashard cluster3
                handle_intra_shard(x, y, amount, cluster3_port)

            else:
                
                if x in cluster1:
                    if y in cluster2:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster1_port, cluster2_port)

                    elif y in cluster3:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster1_port, cluster3_port)

                    else:
                        #abort
                        print(f"Server {y} does not exist!")

                elif x in cluster2:

                    if y in cluster1:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster2_port, cluster1_port)

                    elif y in cluster3:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster2_port, cluster3_port)

                    else:
                        # abort
                        print(f"Server {y} does not exist!")
                        
                
                elif x in cluster3:

                    if y in cluster1:
                       handle_cross_shard(x, y, amount, cluster3_port, cluster1_port)
                        
                    elif y in cluster2:
                       handle_cross_shard(x, y, amount, cluster3_port, cluster2_port)
                        
                    else:
                        # abort
                        print(f"Server {y} does not exist!")
                        
                else:
                    # abort
                    print(f"Server {x} does not exist!")
                    