import socket
import argparse
import logging
from communication_factory import CommunicationFactory
from election.state_manager import StateManager
from data_manager import DataManager
from election.timeout_manager import TimeoutManager


def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number
    comm_factory = CommunicationFactory()
    internal_state = StateManager()
    data_manager = DataManager(args.cluster)
    limit = 2

    # Starting Server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()
    print("Listening on port: {}".format(port))

    timeout_manager = TimeoutManager()

    comm_factory.receive(server, limit, internal_state, timeout_manager)

    
    # Start Timeouts -> needs to keep happening in a loop





if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    # %(asctime)s - %(levelname)s - 
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=int, default=8000)
    parser.add_argument('-cluster', type=int, default=None)
    # parser.add_argument('-balance', type=float, default=10.0)
    args = parser.parse_args()
    run_server(args)