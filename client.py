import threading
import socket
import time
import csv
from collections import defaultdict
from utils import object_to_txt
from enum import Enum
from bullet import Bullet
from bullet import colors
import json
from election.state_manager import LogEntry, Transaction

class TransactionEnum(Enum):
    INTRA_SHARD = 1
    CROSS_SHARD = 2

class PhaseState:
    def __init__(self):
        self.is_prepare_phase = False
        self.is_commit_phase = False
        self.is_timer_running = False
        self.start_time = time.time()
        self.stop_timer = False
    
    def stop_running_timer(self):
        self.stop_timer = True
    
    def reset_timer(self):
        self.start_time = time.time()
    



def prepare_monitor(client1, client2, transaction, x, y, prepare_phase, ack_messages1, ack_messages2, phase_state: PhaseState, start_time, latencies):

    while True:


        if phase_state.is_prepare_phase and len(prepare_phase) != 0:

            messages = set(prepare_phase)

            if "PREPARE_FAIL" in messages:
                # print("prepare fail")
                print(f"[CLIENT ID {transaction.client_id}] Received Prepare Fail ...")
                print(f"[CLIENT ID {transaction.client_id}] Sending Abort Message ...")

                stop_and_send_abort(client1, client2, transaction, x, y)
                prepare_phase.clear()
                phase_state.is_prepare_phase = False
                phase_state.is_commit_phase = True
                # phase_state.stop_running_timer()
                phase_state.reset_timer()

            else:
                if len(prepare_phase) == 4:
                    # print("Sent Commit")
                    print(f"[CLIENT ID {transaction.client_id}] Received Prepare Messages ...")
                    print(f"[CLIENT ID {transaction.client_id}] Sending Commit Message ...")
                    transaction.x = x
                    transaction.y = 0
                    client1.send(bytes(f"SERVER_RELAY@{transaction.client_id}#COMMIT|{object_to_txt(transaction)}|@", "utf-8"))

                    transaction.y = y
                    transaction.x = 0
                    client2.send(bytes(f"SERVER_RELAY@{transaction.client_id}#COMMIT|{object_to_txt(transaction)}|@", "utf-8"))
                    transaction.x = x
                    transaction.y = y
                    prepare_phase.clear()
                    phase_state.is_prepare_phase = False
                    phase_state.is_commit_phase = True
                    phase_state.reset_timer()


        
        if phase_state.is_commit_phase and len(ack_messages1) + len(ack_messages2) != 0:
            ack_messages = ack_messages1 + ack_messages2
            if len(ack_messages) == 6:
                if len(set(ack_messages)) == 1:
                    if list(set(ack_messages))[0] == "ACK_SUCCESS":
                        calculate_latency(start_time, latencies, transaction)
                        print(f"[CLIENT ID {transaction.client_id}] 2PC Succeeded: {x}, {y}, {transaction.amount}")
                        phase_state.stop_running_timer()

                        break
                    elif list(set(ack_messages))[0] == "ACK_FAIL":
                        # print("2PC Aborted")
                        calculate_latency(start_time, latencies, transaction)
                        print(f"[CLIENT ID {transaction.client_id}] 2PC Aborted: {x}, {y}, {transaction.amount}")
                        phase_state.stop_running_timer()
                        break


def transaction_coordinator(client, prepare_phase, ack_messages, phase_state: PhaseState):
    
    while True:
        message = client.recv(6000).decode("utf-8")

        # Have a timeout for this
        if message == "PREPARE_SUCCESS":
            if phase_state.is_prepare_phase:
                prepare_phase.append("PREPARE_SUCCESS")
                phase_state.reset_timer()
            
        elif message == "PREPARE_FAIL":
            if phase_state.is_prepare_phase:
                prepare_phase.append("PREPARE_FAIL")
                phase_state.reset_timer()

        elif message == "ACK_FAIL":
            if phase_state.is_commit_phase:
                # print(message)
                ack_messages.append("ACK_FAIL")
                if len(ack_messages) == 3:
                    break
            # if len(ack_messages) == 2:
            #     break
        

        elif message == "ACK_SUCCESS":
            if phase_state.is_commit_phase:
                # print(message)
                ack_messages.append("ACK_SUCCESS")
                if len(ack_messages) == 3:
                    break
            # if len(ack_messages) == 2:
            #     break



def stop_and_send_abort(client1, client2, transaction, x, y):

    transaction.x = x
    transaction.y = 0
    client1.send(bytes(f"SERVER_RELAY@{transaction.client_id}#ABORT|{object_to_txt(transaction)}|@", "utf-8"))

    transaction.y = y
    transaction.x = 0
    client2.send(bytes(f"SERVER_RELAY@{transaction.client_id}#ABORT|{object_to_txt(transaction)}|@", "utf-8"))
    transaction.x = x
    transaction.y = y

def timer(phase_state: PhaseState, client1, client2, transaction, x, y):

    time_limit = 30  # Timeout after 5 seconds

    while True:

        if phase_state.stop_timer == True:
            break

        while phase_state.is_prepare_phase and phase_state.is_timer_running and phase_state.stop_timer == False:
            elapsed_time = time.time() - phase_state.start_time
            if elapsed_time > time_limit:
                phase_state.is_timer_running = False
                phase_state.stop_timer = True
                phase_state.is_commit_phase = True
                phase_state.is_prepare_phase = False
                stop_and_send_abort(client1, client2, transaction, x, y)
                print("Timeout occurred on Prepare!")
                break
                # self.reset_timer()
            
            time.sleep(2)

        while phase_state.is_commit_phase and phase_state.is_timer_running and phase_state.stop_timer == False:
            elapsed_time = time.time() - phase_state.start_time
            if elapsed_time > time_limit:
                phase_state.is_timer_running = False
                phase_state.stop_timer = True
                stop_and_send_abort(client1, client2, transaction, x, y)
                print("Timeout occurred on Acks!")
                break
                # self.reset_timer()
            
            time.sleep(2)


def calculate_latency(start_time, latencies, transaction):
    elapsed_time = time.time() - start_time
    print(f"[CLIENT ID {transaction.client_id}] Transaction Latency: {round(elapsed_time, 2)} seconds")
    latencies.append(elapsed_time)

def handle(client, start_time, latencies, transaction, x, y):
    
    while True:
        message = client.recv(6000).decode("utf-8")

        if message == "COMMIT_SUCCESS":
            calculate_latency(start_time, latencies, transaction)
            print(f"[CLIENT ID {transaction.client_id}] Intra Shard Succeeded: {x}, {y}, {transaction.amount}")
            client.close()
            break
            
        elif message == "INSUFFICIENT_FUNDS":
            calculate_latency(start_time, latencies, transaction)
            print(f"[CLIENT ID {transaction.client_id}] Intra Shard Failed - Insufficient Funds: {x}, {y}, {transaction.amount}")
            client.close()
            break

        elif message == "ABORT":
            calculate_latency(start_time, latencies, transaction)
            print(f"[CLIENT ID {transaction.client_id}] Intra Shard Failed: {x}, {y}, {transaction.amount}")
            client.close()
            break


def handle_cross_shard(x, y, amount, cluster_port1, cluster_port2, latencies):
    
    start_time = time.time()
    prepare_phase = []
    ack_messages1 = []
    ack_messages2 = []
    phase_state = PhaseState()
    phase_state.is_prepare_phase = True
    phase_state.is_timer_running = True


    transaction = Transaction(x, y, amount, 'cross_shard')
    print(f"[CLIENT ID {transaction.client_id}] Sent Cross Shard Message: {x}, {y}, {amount}")
    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect(('localhost', cluster_port1))
    transaction.y = 0
    clientsocket1.send(bytes(f"CLIENT_INIT@{transaction.client_id}#PREPARE|{object_to_txt(transaction)}|@", "utf-8"))


    clientsocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket2.connect(('localhost', cluster_port2))
    transaction.y = y
    transaction.x = 0 
    clientsocket2.send(bytes(f"CLIENT_INIT@{transaction.client_id}#PREPARE|{object_to_txt(transaction)}|@", "utf-8"))
    transaction.x = x
    transaction.y = y

    phase_state.start_time = time.time()

    thread1 = threading.Thread(target=transaction_coordinator, args=(clientsocket1, prepare_phase, ack_messages1, phase_state, ))
    thread1.start()

    thread2 = threading.Thread(target=transaction_coordinator, args=(clientsocket2, prepare_phase, ack_messages2, phase_state, ))
    thread2.start()

    thread = threading.Thread(target=prepare_monitor, args=(clientsocket1, clientsocket2, transaction, x, y, prepare_phase, ack_messages1, ack_messages2, phase_state, start_time, latencies, ))
    thread.start()

    timer_thread = threading.Thread(target=timer, args=(phase_state, clientsocket1, clientsocket2, transaction, x, y, ))
    timer_thread.start()

    

def handle_intra_shard(x, y, amount, cluster_port, latencies):
    start_time = time.time()
    transaction = Transaction(x, y, amount, 'intra_shard')
    print(f"[CLIENT ID {transaction.client_id}] Sent Intra Shard Message: {x}, {y}, {amount}")
    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect(('localhost', cluster_port))
    clientsocket1.send(bytes(f"CLIENT_INIT@{transaction.client_id}#CLIENT|{object_to_txt(transaction)}|@", "utf-8"))

    thread = threading.Thread(target=handle, args=(clientsocket1, start_time, latencies, transaction, x, y, ))
    thread.start()


def menu():

    def sanity_check(x, y):
        cluster1 = set(list(range(1, 1001)))
        cluster2 = set(list(range(1001, 2001)))
        cluster3 = set(list(range(2001, 3001)))

        if (x in cluster1 and y in cluster1) or (x in cluster2 and y in cluster2) or (x in cluster3 and y in cluster3):
            return True
        else:
            return False
        
    def cross_shard_sanity_check(x, y):
        cluster1 = set(list(range(1, 1001)))
        cluster2 = set(list(range(1001, 2001)))
        cluster3 = set(list(range(2001, 3001)))

        if (x in cluster1 and y in cluster1) or (x in cluster2 and y in cluster2) or (x in cluster3 and y in cluster3):
            return False
        else:
            if (x not in cluster1 and x not in cluster2 and x not in cluster3):
                print(f"Account ID {x} does not exist!")
                return False
            elif  (y not in cluster1 or y not in cluster2 or y not in cluster3):
                print(f"Account ID {y} does not exist!")
                return False
            return True

    while True:

        from bullet import Bullet

        cli = Bullet(
                prompt = "\nPlease choose a command: ",
                choices = ["Execute transactions from a file", "Intra Shard Transaction", "Cross Shard Transaction", "PrintBalance", "PrintDataStore"], 
                indent = 0,
                align = 2, 
                margin = 2,
                shift = 0,
                bullet = "",
                pad_right = 2,
                return_index=True
            )

        prompt, index = cli.launch()

        if index == 0:
            execute_transaction_file('transaction.csv')
        
        elif index == 1:
            x = int(input("Enter Withdrawal Account ID: "))
            y = int(input("Enter Deposit Account ID: "))
            amount = float(input("Enter Amount: "))

            if sanity_check(x, y):
                intra_shard_wrapper(x, y, amount)
            else:
                print(f"Error: Account IDs do not belong to the same cluster: {x}, {y}")
        
        elif index == 2:
            x = int(input("Enter Withdrawal Account ID: "))
            y = int(input("Enter Deposit Account ID: "))
            amount = float(input("Enter Amount: "))

            if cross_shard_sanity_check(x, y):
                cross_shard_wrapper(x, y, amount)
            else:
                print(f"Error: Account IDs belong to the same cluster: {x}, {y}")
        
        elif index == 3:

            x = int(input("Enter Account ID: "))
            print_balance(x)

        elif index == 4:
            
            x = int(input("Enter Account ID: "))
            print_data_store(x)

def print_data_store(x):
    
    def load_from_file(filename):
        with open(filename, "r") as file:
            data = json.load(file)

        current_term, voted_for, logs, last_applied, last_cross_shard = data["current_term"], data["voted_for"], data["log_entries"], data["last_applied"], data["last_cross_shard_applied"]
        log_entries = []
        for log in logs:
            log_entries.append(LogEntry.from_dict(log))
        return current_term, voted_for, log_entries, last_applied, last_cross_shard
    

    _, _, logs, last_applied, last_cross_shard = load_from_file(f"/Users/vamsi/Documents/GitHub/raft-2pc/data/metadata/c{(x - 1)//3 + 1}s{x}_metadata.json")

    output = []
    cross_shard_output = []

    for log in logs[:last_applied]:
        if log.command.type != "cross_shard":
            output.append(log)  
    
    for log in logs[:last_cross_shard]:
        if log.command.type == "cross_shard":
            cross_shard_output.append(log)

    print(f"Intra Shard Committed Transactions")
    for i, log in enumerate(output):
        print(f"{i + 1}. {log}")

    print(f"Cross Shard Committed Transactions")

    for i, log in enumerate(cross_shard_output):
        print(f"{i + 1}. {log}")





def print_balance(x):
    cluster1 = set(list(range(1, 1001)))
    cluster2 = set(list(range(1001, 2001)))
    cluster3 = set(list(range(2001, 3001)))

    def read_data(i, output, cluster_no):
        with open(f'/Users/vamsi/Documents/GitHub/raft-2pc/data/cluster{cluster_no}_server{i}_data.csv', 'r') as file:
            csv_reader = csv.reader(file)
            csv_reader.__next__()
            for row in csv_reader:
                id, balance = row
                id, balance = int(id), float(balance)
                if x == id:
                    output.append(f"Server{i} Balance of Client: {x} = {balance}")

    output = []
    server_range = None
    cluster_no = None
    if x in cluster1:
        cluster_no = 1
        server_range = range(1, 4)
    elif x in cluster2:
        cluster_no = 2
        server_range = range(4, 7)
    elif x in cluster3:
        cluster_no = 3
        server_range = range(7, 10)
    else:
        print(f"Account ID {x} does not exist in any cluster")
        return
    
    for i in server_range:
        read_data(i, output, cluster_no)

    for log in output:
        print(log)

def cross_shard_wrapper(x, y, amount):
    cluster1_port = 8080
    cluster2_port = 8081
    cluster3_port = 8082


    cluster1 = set(list(range(1, 1001)))
    cluster2 = set(list(range(1001, 2001)))
    cluster3 = set(list(range(2001, 3001)))
    latencies = []

    if x in cluster1:
        if y in cluster2:
            # cross shard
            handle_cross_shard(x, y, amount, cluster1_port, cluster2_port, latencies)

        elif y in cluster3:
            # cross shard
            handle_cross_shard(x, y, amount, cluster1_port, cluster3_port, latencies)

        else:
            #abort
            print(f"Server {y} does not exist!")

    elif x in cluster2:

        if y in cluster1:
            # cross shard
            handle_cross_shard(x, y, amount, cluster2_port, cluster1_port, latencies)

        elif y in cluster3:
            # cross shard
            handle_cross_shard(x, y, amount, cluster2_port, cluster3_port, latencies)

        else:
            # abort
            print(f"Server {y} does not exist!")
            
    
    elif x in cluster3:

        if y in cluster1:
            handle_cross_shard(x, y, amount, cluster3_port, cluster1_port, latencies)
            
        elif y in cluster2:
            handle_cross_shard(x, y, amount, cluster3_port, cluster2_port, latencies)
            
        else:
            # abort
            print(f"Server {y} does not exist!")
            
    else:
        # abort
        print(f"Server {x} does not exist!")

def intra_shard_wrapper(x, y, amount):
    cluster1_port = 8080
    cluster2_port = 8081
    cluster3_port = 8082


    cluster1 = set(list(range(1, 1001)))
    cluster2 = set(list(range(1001, 2001)))
    cluster3 = set(list(range(2001, 3001)))
    latencies = []

    if x in cluster1 and y in cluster1:
        # intrashard cluster1
        handle_intra_shard(x, y, amount, cluster1_port, latencies)
        # time.sleep(0.5)
        
    elif x in cluster2 and y in cluster2:
        # intrashard cluster2
        handle_intra_shard(x, y, amount, cluster2_port, latencies)

    elif x in cluster3 and y in cluster3:
        # intrashard cluster3
        handle_intra_shard(x, y, amount, cluster3_port, latencies)

def execute_transaction_file(filename):

    cluster1 = set(list(range(1, 1001)))
    cluster2 = set(list(range(1001, 2001)))
    cluster3 = set(list(range(2001, 3001)))


    cluster1_port = 8080
    cluster2_port = 8081
    cluster3_port = 8082

    latencies = []
    no_of_transactions = 0
    flag = 0
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        # csv_reader.__next__()


        for row in csv_reader:
            x, y, amount = row
            x, y, amount = int(x), int(y), float(amount)
            no_of_transactions += 1

            if x == y:
                print("Self transaction is not possible!")
                flag = 1

            elif x in cluster1 and y in cluster1:
                # intrashard cluster1
                handle_intra_shard(x, y, amount, cluster1_port, latencies)
                # time.sleep(0.5)
                
            elif x in cluster2 and y in cluster2:
                # intrashard cluster2
                handle_intra_shard(x, y, amount, cluster2_port, latencies)

            elif x in cluster3 and y in cluster3:
                # intrashard cluster3
                handle_intra_shard(x, y, amount, cluster3_port, latencies)

            else:
                
                if x in cluster1:
                    if y in cluster2:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster1_port, cluster2_port, latencies)

                    elif y in cluster3:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster1_port, cluster3_port, latencies)

                    else:
                        #abort
                        flag = 1
                        print(f"Server {y} does not exist!")

                elif x in cluster2:

                    if y in cluster1:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster2_port, cluster1_port, latencies)

                    elif y in cluster3:
                        # cross shard
                       handle_cross_shard(x, y, amount, cluster2_port, cluster3_port, latencies)

                    else:
                        # abort
                        flag = 1
                        print(f"Server {y} does not exist!")
                        
                
                elif x in cluster3:

                    if y in cluster1:
                       handle_cross_shard(x, y, amount, cluster3_port, cluster1_port, latencies)
                        
                    elif y in cluster2:
                       handle_cross_shard(x, y, amount, cluster3_port, cluster2_port, latencies)
                        
                    else:
                        # abort
                        flag = 1
                        print(f"Server {y} does not exist!")
                        
                else:
                    # abort
                    flag = 1
                    print(f"Server {x} does not exist!")
        

        while len(latencies) != no_of_transactions:
            if flag == 1:
                break
            continue
            
        if flag == 0:
            if sum(latencies) != 0:
                print(f"Throughput: {round(no_of_transactions/sum(latencies), 2)} transactions/second")



if __name__ == "__main__":

    menu()
