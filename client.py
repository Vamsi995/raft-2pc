import threading
import socket
import time
import csv
from collections import defaultdict
from utils import object_to_txt


class ClientManager:

    def __init__(self):
        self.clients = set()
        self.client_candidate_map = {}
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('localhost', 8081))
        self.server.listen()
        self.cluster_candidate = defaultdict(list)
        self.cluster1 = set(list(range(1, 1001)))
        self.cluster2 = set(list(range(1001, 2001)))
        self.cluster3 = set(list(range(2001, 3001)))
        self.cluster1_servers = set([1, 2, 3])
        self.cluster2_servers = set([4, 5, 6])
        self.clsuter3_servers = set([7, 8, 9])

    
    def receive(self, ):
        counter = 0
        while True:
        # Accept Connection
            client, _ = self.server.accept()
            counter += 1
            print("Connected with {}".format(client.getpeername()))
            self.clients.add(client)

            thread = threading.Thread(target=self.handle, args=(client, ))
            thread.start()

            if counter == 3:
                break

    def broadcast(self, message, client):

        for c in self.clients:
            if client != c: 
                time.sleep(1)
                c.send(bytes(message, "utf-8"))
    
    def send_to(self, message, client_id):
        
        for client, candidate_num in self.client_candidate_map.items():
            if candidate_num == client_id:
                time.sleep(1)
                client.send(bytes(message, "utf-8"))
                break

    def handle_argument(self, protocol, client, obj):
         
        if protocol == "BROADCAST":
            self.broadcast(obj, client)

        elif protocol == "INIT":
            candidate_num = int(obj)
            self.cluster_candidate[(candidate_num - 1)//3].append(client)
            # self.client_candidate_map[client] = candidate_num 
        
        elif protocol == "RELAY":
            candidate_num, piggy_back_obj = obj.split("#")
            candidate_num = int(candidate_num)
            self.send_to(piggy_back_obj, candidate_num)
        elif protocol == '':
            return 


    def handle(self, client):

        # If term > currentTerm, currentTerm ← term
        # (step down if leader or candidate)
        # 2. If term == currentTerm, votedFor is null or candidateId,
        # and candidate's log is at least as complete as local log,
        # grant vote and reset election timeout

        while True:
            try:
                # Broadcasting Messages
                message = client.recv(4096).decode("utf-8")
                if not message: raise Exception("Disconnected Socket")
                print(message)

                message_split = message.split("@")
                print(message_split)
                i = 0

                while i < len(message_split):
                        if i + 1 > len(message_split) - 1:
                            break
                        self.handle_argument(message_split[i], client, message_split[i + 1])
                        i += 2

                # message, piggy_back_obj = message.split("@")

                # if message == "BROADCAST":
                #     self.broadcast(piggy_back_obj, client)

                # elif message == "INIT":
                #     candidate_num = int(piggy_back_obj)
                #     self.client_candidate_map[client] = candidate_num 
                
                # elif message == "RELAY":
                #     candidate_num, piggy_back_obj = piggy_back_obj.split("#")
                #     candidate_num = int(candidate_num)
                #     self.send_to(piggy_back_obj, candidate_num)


            except Exception as e:
                # print(e)
                # Removing And Closing Clients
                self.clients.remove(client)
                self.client_candidate_map.pop(client)
                client.close()
                break

class Transaction:

    def __init__(self, x, y, amount, client_id=None):
        self.x = x
        self.y = y
        self.amount = amount
        if client_id == None:
            self.client_id = id(self)
        else:
            self.client_id = client_id
    
    def to_dict(self):
        return {
            'x': self.x,
            'y': self.y,
            'amount': self.amount,
            'client_id': self.client_id
        }

    @classmethod
    def from_dict(cls, dict_obj):
        return cls(dict_obj['x'], dict_obj['y'], dict_obj['amount'], dict_obj['client_id'])


def handle(client):
    
    while True:
        message = client.recv(6000).decode("utf-8")
            # message, piggy_back_obj = message.split("|")

        if message == "Success":
            print(message)
            break
        else:
            print("Abort!")
            break
        # message_split = message.split("|")



if __name__ == "__main__":

    # comm_manager: ClientManager = ClientManager()
    # comm_manager.receive()

    time.sleep(10)

    cluster1 = set(list(range(1, 1001)))
    cluster2 = set(list(range(1001, 2001)))
    cluster3 = set(list(range(2001, 3001)))

    cluster1_servers = [1, 2, 3]

    cluster1_port = 8080


    with open('transaction.csv', 'r') as file:
        csv_reader = csv.reader(file)
        # csv_reader.__next__()
        for row in csv_reader:
            x, y, amount = row
            x, y, amount = int(x), int(y), float(amount)



            if int(x) in cluster1 and int(y) in cluster1:
                # intrashard cluster1
                # client = comm_manager.cluster_candidate[0][0]
                transaction = Transaction(x, y, amount)
                # message = f"CLIENT|{object_to_txt(transaction)}"
                print("Sent Message")
                clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                clientsocket1.connect(('localhost', cluster1_port))
                clientsocket1.send(bytes(f"CLIENT_INIT@{transaction.client_id}#CLIENT|{object_to_txt(transaction)}|@", "utf-8"))

                thread = threading.Thread(target=handle, args=(clientsocket1, ))
                thread.start()

                # time.sleep(6)
                # clientsocket1.send(bytes(message, "utf-8"))
                
            elif x in cluster2 and y in cluster2:
                # intrashard cluster2
                pass
            elif x in cluster3 and y in cluster3:
                # intrashard cluster3
                pass
            else:
                pass
