import threading
import socket

CLIENTS = []


def receive(server):
    while True:
        # Accept Connection
        client, address = server.accept()
        print("Connected with {}".format(client.getpeername()))
        CLIENTS.append(client)

        

        # # Start Handling Thread For Client
        # thread = threading.Thread(target=self.handle, args=(client))
        # thread.start()

def run_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 8080))
    server.listen()

    receive(server)


    # pass 


if __name__ == "__main__":
    run_server()