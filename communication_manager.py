import threading
import socket
import time
import random
from collections import defaultdict, deque
import argparse

class CommunicationManager:

    def __init__(self, port, cluster_id):
        self.clients = set()
        self.client_candidate_map = {}
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('localhost', port))
        self.server.listen()
        self.cluster_id = cluster_id
        self.clients_map = {}

        self.client_pc_ack = defaultdict(list)
        self.request_queues = defaultdict(list)

    
    def receive(self, ):
        while True:
        # Accept Connection
            client, _ = self.server.accept()
            print("Connected with {}".format(client.getpeername()))
            self.clients.add(client)

            self.request_queues[client] = deque()
            thread = threading.Thread(target=self.handle, args=(client, self.request_queues, ))
            thread.start()

            thread = threading.Thread(target=self.handle_argument, args=(self.request_queues[client], ))
            thread.start()

    def multicast(self, message):

        for c, _ in self.client_candidate_map.items():
            c.send(bytes(message, "utf-8"))


    def broadcast(self, message, client=None):

        for c in self.clients:
            if client != c: 
                # time.sleep(1)
                c.send(bytes(message, "utf-8"))
    
    def send_to(self, message, client_id):
        
        for client, candidate_num in self.client_candidate_map.items():
            if candidate_num == client_id:
                # time.sleep(2)
                # print(client_id, message)

                client.send(bytes(message, "utf-8"))
                break

    def handle_argument(self, request_queue):

        while True:

            while len(request_queue) != 0:
                protocol, client, obj = request_queue[0]

                if protocol == "BROADCAST":
                    self.broadcast(obj, client)

                elif protocol == "INIT":
                    candidate_num = int(obj)
                    self.client_candidate_map[client] = candidate_num 
                
                elif protocol == "RELAY":
                    candidate_num, piggy_back_obj = obj.split("#")
                    candidate_num = int(candidate_num)
                    self.send_to(piggy_back_obj, candidate_num)
                
                elif protocol == "SERVER_RELAY":
                    client_id, piggy_back_obj = obj.split("#")            
                    print(client_id, piggy_back_obj)
                    # candidate_num = random.choice(list(self.client_candidate_map.values()))
                    self.multicast(piggy_back_obj)


                elif protocol == "CLIENT_INIT":
                    client_id, piggy_back_obj = obj.split("#")
                    self.clients_map[int(client_id)] = client
                    # candidate_num = 1
                    candidate_num = random.choice(list(self.client_candidate_map.values()))
                    self.send_to(piggy_back_obj, candidate_num)

                elif protocol == "CLIENT_RELAY":
                    # print("committed")
                    client_id, piggy_back_obj = obj.split("#")
                    out_client = self.clients_map[int(client_id)]
                    out_client.send(bytes(piggy_back_obj, "utf-8"))
                    # self.clients_map.pop(client_id)

                elif protocol == "CLIENT_RELAY_ACK":
                    client_id, piggy_back_obj = obj.split("#")
                    out_client = self.clients_map[int(client_id)]
                    out_client.send(bytes(piggy_back_obj, "utf-8"))
                    # client_id, piggy_back_obj = obj.split("#")
                    # print(self.client_pc_ack[int(client_id)], len(self.client_candidate_map))
                    # if len(self.client_pc_ack[int(client_id)]) != len(self.client_candidate_map):
                    #     self.client_pc_ack[int(client_id)].append(piggy_back_obj)
                    #     request_queue.popleft()
                    #     continue

                    # if len(set(self.client_pc_ack[int(client_id)])) == 1:
                    #     out_client = self.clients_map[int(client_id)]
                    #     out_client.send(bytes(piggy_back_obj, "utf-8"))
                        # self.clients_map.pop(client_id)


                request_queue.popleft()


    def handle(self, client, request_queues):

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
                # print(message)

                message_split = message.split("@")
                # print(message_split)
                i = 0

                while i < len(message_split):
                        if i + 1 > len(message_split) - 1:
                            break
                        request_queues[client].append([message_split[i], client, message_split[i + 1]])
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
                if client in self.clients:
                    self.clients.remove(client)
                
                if client in self.client_candidate_map:
                    self.client_candidate_map.pop(client)
                
                for client_id, cl in self.clients_map.items():
                    if cl == client:
                        self.clients_map.pop(client_id)
                        break

                client.close()
                break


def main(args):
    comm_manager = CommunicationManager(args.port, args.cluster)
    comm_manager.receive()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=int, default=8000)
    parser.add_argument('-cluster', type=int, default=None)
    parser.add_argument('-candidate_id', type=int, default=0)
    parser.add_argument('-balance', type=float, default=10.0)
    args = parser.parse_args()
    main(args)
    