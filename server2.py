import socket
import argparse
import threading
# from election.state_manager import StateManager
# from communication_factory import CommunicationFactory
import logging
# from data_manager import DataManager



def handle(client):

    # If term > currentTerm, currentTerm ← term
    # (step down if leader or candidate)
    # 2. If term == currentTerm, votedFor is null or candidateId,
    # and candidate's log is at least as complete as local log,
    # grant vote and reset election timeout

    while True:
        try:
            # Broadcasting Messages
            message = client.recv(4096).decode("utf-8")
            print(message)
            # message, piggy_back_obj = message.split("|")
                        
                
        except Exception as e:
            print(e)
            # Removing And Closing Clients
            self.clients.remove(client)
            client.close()
            break



def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number


    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect((host, 8080))
    clientsocket1.send(bytes(f"INIT|{args.candidate_id}", "utf-8"))


    thread = threading.Thread(target=handle, args=(clientsocket1, ))
    thread.start()
    # comm_factory = CommunicationFactory()
    # internal_state = StateManager()
    # data_manager = DataManager(args.cluster)

    # limit = 2


    # clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # clientsocket.connect((host, 8001))
    # comm_factory.CLIENTS.append(clientsocket)

    # print("Connected with {}".format(clientsocket.getpeername()))

    # thread = threading.Thread(target=comm_factory.handle, args=(clientsocket, internal_state))
    # thread.start()
    

    # # Starting Server
    # server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server.bind((host, port))
    # server.listen()
    # print("Listening on port: {}".format(port))

    # comm_factory.receive(server, limit, internal_state)
    

    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')


    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=int, default=8000)
    parser.add_argument('-cluster', type=int, default=None)
    parser.add_argument('-candidate_id', type=int, default=0)
    parser.add_argument('-balance', type=float, default=10.0)
    args = parser.parse_args()
    run_server(args)