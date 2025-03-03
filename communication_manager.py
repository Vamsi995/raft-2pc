import threading
import socket
import time
import random

class CommunicationManager:

    def __init__(self):
        self.clients = set()
        self.client_candidate_map = {}
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('localhost', 8080))
        self.server.listen()

        self.clients_map = {}

    
    def receive(self, ):
        while True:
        # Accept Connection
            client, _ = self.server.accept()
            print("Connected with {}".format(client.getpeername()))
            self.clients.add(client)

            thread = threading.Thread(target=self.handle, args=(client, ))
            thread.start()

    def broadcast(self, message, client):

        for c in self.clients:
            if client != c: 
                time.sleep(1)
                c.send(bytes(message, "utf-8"))
    
    def send_to(self, message, client_id):
        
        for client, candidate_num in self.client_candidate_map.items():
            if candidate_num == client_id:
                # time.sleep(2)
                # print(client_id, message)

                client.send(bytes(message, "utf-8"))
                break

    def handle_argument(self, protocol, client, obj):
         
        if protocol == "BROADCAST":
            self.broadcast(obj, client)

        elif protocol == "INIT":
            candidate_num = int(obj)
            self.client_candidate_map[client] = candidate_num 
        
        elif protocol == "RELAY":
            candidate_num, piggy_back_obj = obj.split("#")
            candidate_num = int(candidate_num)
            self.send_to(piggy_back_obj, candidate_num)

        elif protocol == "CLIENT_INIT":
            client_id, piggy_back_obj = obj.split("#")            
            self.clients_map[int(client_id)] = client
            candidate_num = random.choice(list(self.client_candidate_map.values()))
            self.send_to(piggy_back_obj, candidate_num)

        elif protocol == "COMMITTED":
            # print("committed")
            client_id = int(obj)
            out_client = self.clients_map[client_id]
            out_client.send(bytes("Success", "utf-8"))
            self.clients_map.pop(client_id)

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
                # print(message)

                message_split = message.split("@")
                # print(message_split)
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



if __name__ == "__main__":
    comm_manager = CommunicationManager()
    comm_manager.receive()