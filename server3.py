import socket
import argparse
import threading
import logging
from communication_factory import CommunicationFactory
from state_manager import StateManager
from data_manager import DataManager

def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number
    comm_factory = CommunicationFactory()
    internal_state = StateManager()
    data_manager = DataManager(args.cluster)

    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect((host, 8001))
    comm_factory.CLIENTS.append(clientsocket1)
    print("Connected with {}".format(clientsocket1.getpeername()))

    thread = threading.Thread(target=comm_factory.handle, args=(clientsocket1, internal_state))
    thread.start()

    clientsocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket2.connect((host, 8002))
    comm_factory.CLIENTS.append(clientsocket2)
    print("Connected with {}".format(clientsocket2.getpeername()))

    thread = threading.Thread(target=comm_factory.handle, args=(clientsocket2, internal_state))
    thread.start()

    print("Initialized all connections!")

    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=int, default=8000)
    parser.add_argument('-cluster', type=int, default=None)
    # parser.add_argument('-balance', type=float, default=10.0)
    args = parser.parse_args()
    run_server(args)