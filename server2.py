import socket
import argparse
import threading
from election.state_manager import StateManager
from communication_factory import CommunicationFactory
import logging
from data_manager import DataManager

def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number
    comm_factory = CommunicationFactory()
    internal_state = StateManager()
    data_manager = DataManager(args.cluster)

    limit = 2


    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect((host, 8001))
    comm_factory.CLIENTS.append(clientsocket)

    print("Connected with {}".format(clientsocket.getpeername()))

    thread = threading.Thread(target=comm_factory.handle, args=(clientsocket, internal_state))
    thread.start()
    

    # Starting Server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    print("Listening on port: {}".format(port))

    comm_factory.receive(server, limit, internal_state)
    

    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')


    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=int, default=8000)
    parser.add_argument('-cluster', type=int, default=None)
    # parser.add_argument('-balance', type=float, default=10.0)
    args = parser.parse_args()
    run_server(args)