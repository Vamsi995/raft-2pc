import socket
import argparse
import logging
import threading
import time
import math
# from communication_factory import CommunicationFactory
from election.state_manager import StateManager, State, LogEntry
# from data_manager import DataManager
from election.election_manager import ElectionManager
from utils import txt_to_object
from data.models.reply_vote import ReplyVote
from data.models.append_entries import AppendEntries, AppendEntriesReply
from client import Transaction
from collections import deque


def handle_client_thread(client_queue):

    while True:

        while len(client_queue) != 0:
            election_manager, piggy_back_obj = client_queue[0]

            transaction: Transaction = txt_to_object(piggy_back_obj)
            print(transaction)
            x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

            if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.lock_table[y] == None and election_manager.state_manager.data_manager.get_balance(x) >= amount:

                election_manager.state_manager.lock_table[x] = client_id
                election_manager.state_manager.lock_table[y] = client_id


                print("Received client message:")

                # message, piggy_back_obj = message.split("|")
                if len(election_manager.state_manager.log_entries) == 0:
                    election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, 1, transaction))
                else:
                    election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, election_manager.state_manager.log_entries[-1].index + 1, transaction))
                    
                election_manager.append_entries(transaction)
                client_queue.popleft()
                break
            
            else:
                # main_client.send(bytes("Transaction Aborted!"))
                # abort request
                continue
                

def handle_argument(request_queue, client_queue):

    while True:

        while len(request_queue) != 0:

            message, client, main_client, election_manager, piggy_back_obj = request_queue[0]

            if message == "CLIENT":
                client_queue.append([election_manager, piggy_back_obj])
                # transaction: Transaction = txt_to_object(piggy_back_obj)
                # print(transaction)
                # x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

                # if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.lock_table[y] == None and election_manager.state_manager.data_manager.get_balance(x) >= amount:

                #     election_manager.state_manager.lock_table[x] = client_id
                #     election_manager.state_manager.lock_table[y] = client_id


                #     print("Received client message:", message)

                #     # message, piggy_back_obj = message.split("|")
                #     if len(election_manager.state_manager.log_entries) == 0:
                #         election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, 1, transaction))
                #     else:
                #         election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, election_manager.state_manager.log_entries[-1].index + 1, transaction))
                        
                #     election_manager.append_entries(transaction)
                
                # else:
                #     # main_client.send(bytes("Transaction Aborted!"))
                #     # abort request
                #     continue

            elif message == "REQUEST_VOTE":
                print(piggy_back_obj)
                pass

            elif message == "REPLY_VOTE":
                print(message)
                reply_vote: ReplyVote = txt_to_object(piggy_back_obj)
                print(reply_vote.vote, reply_vote.term, election_manager.votes_collected, reply_vote.candidate_id)

                if reply_vote.vote:
                    election_manager.votes_collected += 1
                    if election_manager.votes_collected > election_manager.majority:
                        election_manager.switch_states(State.LEADER)
                else:
                    if election_manager.state_manager.current_term < reply_vote.term:
                        election_manager.switch_states(State.FOLLOWER)
                        election_manager.state_manager.current_term = reply_vote.term
                        election_manager.state_manager.voted_for = None
                        election_manager.leader_id = None
                        election_manager.state_manager.persist()
                        election_manager.reset_timer()
                
            
            elif message == "APPEND_REPLY":
                append_entry_reply: AppendEntriesReply = txt_to_object(piggy_back_obj)
                append_entry_request: AppendEntries = append_entry_reply.append_entries_request
                candidate_id = append_entry_reply.candidate_id


                current_term = election_manager.state_manager.current_term



                if append_entry_reply.success:
                    # Update nextIndex and matchIndex
                    prev_log_index, log_entries_len = append_entry_request.prev_log_ind, len(append_entry_request.log_entires)
                    print(prev_log_index, log_entries_len, len(election_manager.state_manager.log_entries))
                    if prev_log_index + log_entries_len >= election_manager.next_ind[candidate_id]:
                        election_manager.next_ind[candidate_id] = prev_log_index + log_entries_len + 1
                        election_manager.match_ind[candidate_id] = prev_log_index + log_entries_len

                    if log_entries_len > 0:
                        print(f"Debug: Leader {election_manager.state_manager.candidate_id} updated peer {candidate_id}")

                    # Check if a majority of nodes have replicated the entry
                    if (prev_log_index - 1 + log_entries_len < len(election_manager.state_manager.log_entries)) and \
                        (election_manager.state_manager.commit_index < prev_log_index + log_entries_len) and \
                        (election_manager.state_manager.log_entries[prev_log_index - 1 + log_entries_len].term == election_manager.state_manager.current_term):

                        majority = election_manager.majority
                        count = 1
                        sync_followers = []
                        for j in election_manager.candidates:
                            if election_manager.match_ind[j] >= prev_log_index + log_entries_len:
                                count += 1
                                sync_followers.append(j)

                        if count >= majority:
                            election_manager.state_manager.commit_index = prev_log_index + log_entries_len
                            election_manager.state_manager.persist()
                            election_manager.state_manager.apply_committed_entries()
                            # print(f"Debug: Leader {self.me} updated commit index to {self.commit_index}")
                            # self.persist()
            
                            # self.notify_apply()
                            main_client.send(bytes("COMMITTED@", "utf-8"))
                else:
                    if append_entry_reply.term > current_term:
                        election_manager.state_manager.current_term = append_entry_reply.term
                        election_manager.switch_states(State.FOLLOWER)
                        election_manager.state_manager.voted_for = None
                        election_manager.leader_id = None
                        election_manager.reset_timer()
                        election_manager.state_manager.persist()
                        # self.persist()
                    else:
                        # Follower log is inconsistent, decrement nextIndex
                        election_manager.next_ind[append_entry_reply.candidate_id] = max(1, min(append_entry_reply.conflict_ind, len(election_manager.state_manager.log_entries)))
                        # self.next_index[peer_id] = max(1, min(append_entry_reply.conflict_index, len(self.log)))
                    # print(f"Error: Leader {self.me}, peer {peer_id} is not up to date")


            request_queue.popleft()

def handle(client, main_client, election_manager: ElectionManager, request_queue):

    # If term > currentTerm, currentTerm ← term
    # (step down if leader or candidate)
    # 2. If term == currentTerm, votedFor is null or candidateId,
    # and candidate's log is at least as complete as local log,
    # grant vote and reset election timeout

    while True:
        try:
            # Broadcasting Messages
            message = client.recv(6000).decode("utf-8")
            message_split = message.split("|")
            i = 0

            while i < len(message_split):
                    if i + 1 > len(message_split) - 1:
                        break
                    request_queue.append([message_split[i], client, main_client, election_manager, message_split[i + 1]])
                    i += 2

        except Exception as e:
            print(e)
            # Removing And Closing Clients
            self.clients.remove(client)
            client.close()
            break

def handle_client(client, election_manager: ElectionManager):
     while True:
        try:
            message = client.recv(6000).decode("utf-8")
            print("Received client message")

            if election_manager.leader_id == None:
                pass 
                # what to do when there is no leader
            
            if election_manager.leader_id != election_manager.state_manager.candidate_id:
                election_manager.send_to(message, election_manager.leader_id)
                continue

            message, piggy_back_obj = message.split("|")

            while True:
                # if message == "CLIENT":
                transaction: Transaction = txt_to_object(piggy_back_obj)
                print(transaction)
                x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

                if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.lock_table[y] == None and election_manager.state_manager.data_manager.get_balance(x) >= amount:

                    election_manager.state_manager.lock_table[x] = client_id
                    election_manager.state_manager.lock_table[y] = client_id


                    print("Received client message:", message)

                    # message, piggy_back_obj = message.split("|")
                    if len(election_manager.state_manager.log_entries) == 0:
                        election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, 1, transaction))
                    else:
                        election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, election_manager.state_manager.log_entries[-1].index + 1, transaction))
                        
                    election_manager.append_entries(transaction)
                    break
                
                else:
                    # main_client.send(bytes("Transaction Aborted!"))
                    # abort request
                    continue

            
            

        except Exception as e:
            print(e)
            # Removing And Closing Clients
            self.clients.remove(client)
            client.close()
            break


def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number
    # comm_factory = CommunicationFactory()
    request_queue = deque([])
    client_queue = deque([])


    thread = threading.Thread(target=handle_argument, args=(request_queue, client_queue))
    thread.start()

    thread = threading.Thread(target=handle_client_thread, args=(client_queue, ))
    thread.start()


    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect((host, 8080))
    clientsocket1.send(bytes(f"INIT@{args.candidate_id}@", "utf-8"))

    clientsocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket2.connect((host, 8081))
    clientsocket2.send(bytes(f"INIT@{args.candidate_id}@", "utf-8"))



    internal_state = StateManager(args.candidate_id, args.cluster)
    election_manager = ElectionManager(internal_state, clientsocket1, )
    thread = threading.Thread(target=handle, args=(clientsocket1, clientsocket2, election_manager, request_queue))
    thread.start()

    thread = threading.Thread(target=handle_client, args=(clientsocket2, election_manager,))
    thread.start()
    
    election_manager.start_timer()
    # clientsocket1.send(bytes("BROADCAST|testing", "utf-8"))
    # time.sleep(0.1)

    # data_manager = DataManager(args.cluster)

    # Starting Server
    # server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server.bind((host, port))
    # server.listen()
    # print("Listening on port: {}".format(port))
    # comm_factory.receive(server, limit, election_manager)

    
    # Start Timeouts -> needs to keep happening in a loop
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    # %(asctime)s - %(levelname)s - 
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', type=int, default=8000)
    parser.add_argument('-cluster', type=int, default=None)
    parser.add_argument('-candidate_id', type=int, default=0)
    parser.add_argument('-balance', type=float, default=10.0)
    args = parser.parse_args()
    run_server(args)