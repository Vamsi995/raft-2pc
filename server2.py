import socket
import argparse
import threading
from election.state_manager import StateManager, State, LogEntry
from election.election_manager import ElectionManager
# from communication_factory import CommunicationFactory
import logging
from utils import txt_to_object, object_to_txt
from data.models.request_vote import RequestVote
from data.models.reply_vote import ReplyVote
from data.models.append_entries import AppendEntries, AppendEntriesReply
# from data_manager import DataManager
import time
from collections import deque


# def receive(server):
#     counter = 0
#     while True:
#     # Accept Connection
#         client, _ = server.accept()
#         counter += 1
#         print("Connected with {}".format(client.getpeername()))
#         self.clients.add(client)

#         thread = threading.Thread(target=self.handle, args=(client, ))
#         thread.start()

#         if counter == 3:
#             break


def handle_append_entries(append_entries_queue):

    while True:

        while len(append_entries_queue) != 0:

            message, client, election_manager, piggy_back_obj = append_entries_queue[0]

            append_entries: AppendEntries = txt_to_object(piggy_back_obj)

            # if len(append_entries.log_entires) == 0:
            #     # heartbeat
            #     print("Received Heartbeat")
            #     election_manager.leader_id = append_entries.leader_id
            #     election_manager.reset_timer() # if i am a follower or a candidate
            #     continue
            # else:
            #     pass
            print(append_entries.log_entires)
            current_term = election_manager.state_manager.current_term
            candidate_id = election_manager.state_manager.candidate_id

            if append_entries.transaction != None:
                x, y = append_entries.transaction.x, append_entries.transaction.y

                if election_manager.state_manager.lock_table[x] != None or election_manager.state_manager.lock_table[y] != None:
                    continue
                else:
                    election_manager.state_manager.lock_table[x] = append_entries.transaction.client_id
                    election_manager.state_manager.lock_table[y] = append_entries.transaction.client_id

            if current_term > append_entries.term:
                client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, transaction=append_entries.transaction))}|@", "utf-8"))
                # DPrintf("Error, Peer, I : %d DONOT write to log from master: %d, I HAVE BIG TERM MISMATCH: %d \n", rf.me, args.LeaderId, reply.Term)
                print("Error 1")
                continue
        

            election_manager.leader_id = append_entries.leader_id

            if append_entries.term > current_term:
                election_manager.state_manager.current_term = append_entries.term
                election_manager.state_manager.voted_for = None
                # rf.persist() -> Persist to disk
                election_manager.state_manager.persist()
                election_manager.switch_states(State.FOLLOWER)

            election_manager.reset_timer()
            last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()

            if append_entries.prev_log_ind > last_log_ind:
                client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, conflict_ind=last_log_ind + 1, transaction=append_entries.transaction))}|@", "utf-8"))
                print("Error 2")

                continue
            
            log_entries = election_manager.state_manager.log_entries

            if append_entries.prev_log_ind - 1 >= 0 and append_entries.prev_log_ind - 1 < len(log_entries) and log_entries[append_entries.prev_log_ind - 1].term != append_entries.prev_log_term:
                conflict_term = log_entries[append_entries.prev_log_ind].term
                conflict_index = append_entries.prev_log_ind
                while conflict_index > 0 and log_entries[conflict_index - 1].term == conflict_term:
                    conflict_index -= 1
                client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, conflict_ind=conflict_index, conflict_term=conflict_term, transaction=append_entries.transaction))}|@", "utf-8"))                    
                print("Error 3")
                
                continue
            
            i = 0
            for i in range(len(append_entries.log_entires)):
                if append_entries.prev_log_ind + i >= last_log_ind + 1:
                    break

                if append_entries.prev_log_ind + i < len(election_manager.state_manager.log_entries) and election_manager.state_manager.log_entries[append_entries.prev_log_ind + i].term != append_entries.log_entires[i].term:
                    election_manager.state_manager.log_entries = election_manager.state_manager.log_entries[:args.prev_log_index + 1 + i]  # Delete conflicting entries
                    break


            for j in range(i, len(append_entries.log_entires)):
                election_manager.state_manager.log_entries.append(append_entries.log_entires[j])  # Append new entries

            last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()
            if append_entries.commit_ind > election_manager.state_manager.commit_index:
                election_manager.state_manager.commit_index = min(append_entries.commit_ind, last_log_ind)
                # apply_committed_entries()
                election_manager.state_manager.persist()
                election_manager.state_manager.apply_committed_entries()
            
            client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, True, candidate_id, append_entries, transaction=append_entries.transaction))}|@", "utf-8"))                    



            append_entries_queue.popleft()


def handle_argument(request_queue, append_entries_queue):

    while True:

        while len(request_queue) != 0:

            message, client, election_manager, piggy_back_obj = request_queue[0]



            if message == "CLIENT":
                print("Received client message")

                if election_manager.leader_id == None:
                    pass 
                    # what to do when there is no leader
                
                if election_manager.leader_id != election_manager.state_manager.candidate_id:
                    new_message = f"CLIENT|{piggy_back_obj}"
                    election_manager.send_to(new_message, election_manager.leader_id)
                    request_queue.popleft()
                    continue
                # client_queue.append([election_manager, piggy_back_obj])

            elif message == "REQUEST_VOTE":
                request_vote: RequestVote = txt_to_object(piggy_back_obj)
                current_term = election_manager.state_manager.current_term
                voted_for = election_manager.state_manager.voted_for
                candidate_id = election_manager.state_manager.candidate_id

                if current_term == request_vote.term and (voted_for == candidate_id or voted_for == None):
                    time.sleep(1)
                    client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(True, current_term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                    continue
                    
                if request_vote.term < current_term:
                    time.sleep(1)
                    client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(False, current_term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                    continue
                

                if request_vote.term > current_term:
                    election_manager.state_manager.voted_for = None
                    election_manager.state_manager.current_term = request_vote.term
                    # persist to disk
                    election_manager.state_manager.persist()

                    if election_manager.state_manager.state != State.FOLLOWER:
                        election_manager.switch_states(State.FOLLOWER)
                        election_manager.reset_timer()
                        # rf.resetTimer(HEARTBEAT)
                        # rf.currentState = Follower
                    
                
                current_term = election_manager.state_manager.current_term
                voted_for = election_manager.state_manager.voted_for
                candidate_id = election_manager.state_manager.candidate_id

                election_manager.leader_id = None
                last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()

                if (last_log_term > request_vote.last_log_term) or (last_log_term == request_vote.last_log_term and last_log_ind > request_vote.last_log_ind):
                    time.sleep(1)
                    client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(True, request_vote.term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                    continue
                
                time.sleep(1)
                client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(True, request_vote.term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                print("Sent")
                
                election_manager.state_manager.voted_for = request_vote.candidateId
                election_manager.reset_timer()
                election_manager.state_manager.persist()
                # rf.persist() - to disk

                # DPrintf("I: %d, voted true for:  %d\n", rf.me, args.CandidateId)



                        
            elif message == "APPEND_ENTRIES":

                append_entries: AppendEntries = txt_to_object(piggy_back_obj)

                if append_entries.transaction != None:
                    append_entries_queue.append([message, client, election_manager, piggy_back_obj])
                else:

                # if len(append_entries.log_entires) == 0:
                #     # heartbeat
                #     print("Received Heartbeat")
                #     election_manager.leader_id = append_entries.leader_id
                #     election_manager.reset_timer() # if i am a follower or a candidate
                #     continue
                # else:
                #     pass
                # print(append_entries.log_entires)
                # current_term = election_manager.state_manager.current_term
                # candidate_id = election_manager.state_manager.candidate_id

                # if append_entries.transaction != None:
                #     x, y = append_entries.transaction.x, append_entries.transaction.y

                #     if election_manager.state_manager.lock_table[x] != None or election_manager.state_manager.lock_table[y] != None:
                #         continue
                #     else:
                #         election_manager.state_manager.lock_table[x] = append_entries.transaction.client_id
                #         election_manager.state_manager.lock_table[y] = append_entries.transaction.client_id

                    if current_term > append_entries.term:
                        client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries))}|@", "utf-8"))
                        # DPrintf("Error, Peer, I : %d DONOT write to log from master: %d, I HAVE BIG TERM MISMATCH: %d \n", rf.me, args.LeaderId, reply.Term)
                        print("Error 1")
                        continue
                

                    election_manager.leader_id = append_entries.leader_id

                    if append_entries.term > current_term:
                        election_manager.state_manager.current_term = append_entries.term
                        election_manager.state_manager.voted_for = None
                        # rf.persist() -> Persist to disk
                        election_manager.state_manager.persist()
                        election_manager.switch_states(State.FOLLOWER)

                    election_manager.reset_timer()
                    last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()

                    if append_entries.prev_log_ind > last_log_ind:
                        client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, conflict_ind=last_log_ind + 1))}|@", "utf-8"))
                        print("Error 2")

                        continue
                    
                    log_entries = election_manager.state_manager.log_entries

                    if append_entries.prev_log_ind - 1 >= 0 and append_entries.prev_log_ind - 1 < len(log_entries) and log_entries[append_entries.prev_log_ind - 1].term != append_entries.prev_log_term:
                        conflict_term = log_entries[append_entries.prev_log_ind].term
                        conflict_index = append_entries.prev_log_ind
                        while conflict_index > 0 and log_entries[conflict_index - 1].term == conflict_term:
                            conflict_index -= 1
                        client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, conflict_ind=conflict_index, conflict_term=conflict_term))}|@", "utf-8"))                    
                        print("Error 3")
                        
                        continue
                    
                    # i = 0
                    # for i in range(len(append_entries.log_entires)):
                    #     if append_entries.prev_log_ind + i >= last_log_ind + 1:
                    #         break

                    #     if append_entries.prev_log_ind + i < len(election_manager.state_manager.log_entries) and election_manager.state_manager.log_entries[append_entries.prev_log_ind + i].term != append_entries.log_entires[i].term:
                    #         election_manager.state_manager.log_entries = election_manager.state_manager.log_entries[:args.prev_log_index + 1 + i]  # Delete conflicting entries
                    #         break


                    # for j in range(i, len(append_entries.log_entires)):
                    #     election_manager.state_manager.log_entries.append(append_entries.log_entires[j])  # Append new entries

                    last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()
                    if append_entries.commit_ind > election_manager.state_manager.commit_index:
                        election_manager.state_manager.commit_index = min(append_entries.commit_ind, last_log_ind)
                        # apply_committed_entries()
                        election_manager.state_manager.persist()
                        election_manager.state_manager.apply_committed_entries()
                    
                    client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, True, candidate_id, append_entries))}|@", "utf-8"))                    


            request_queue.popleft()

def handle(client, election_manager: ElectionManager, request_queue):

    # If term > currentTerm, currentTerm ← term
    # (step down if leader or candidate)
    # 2. If term == currentTerm, votedFor is null or candidateId,
    # and candidate's log is at least as complete as local log,
    # grant vote and reset election timeout

    while True:
        try:
            # Broadcasting Messages
            message = client.recv(6000).decode("utf-8")
            # message, piggy_back_obj = message.split("|")

            message_split = message.split("|")
            i = 0

            while i < len(message_split):
                    if i + 1 > len(message_split) - 1:
                        break
                    request_queue.append([message_split[i], client, election_manager, message_split[i + 1]])
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

           
            if election_manager.leader_id == None:
                pass 
                # what to do when there is no leader
            
            if election_manager.leader_id != election_manager.state_manager.candidate_id:
                election_manager.send_to(message, election_manager.leader_id)
                continue


            # Broadcasting Messages
            print(message)
            message, piggy_back_obj = message.split("|")

            election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, len(election_manager.state_manager.log_entries), message))
            election_manager.append_entries()

            
            

        except Exception as e:
            print(e)
            # Removing And Closing Clients
            self.clients.remove(client)
            client.close()
            break



def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number
    request_queue = deque([])
    append_entries_queue = deque([])

    thread = threading.Thread(target=handle_argument, args=(request_queue, append_entries_queue, ))
    thread.start()
    
    thread = threading.Thread(target=handle_append_entries, args=(append_entries_queue, ))
    thread.start()

    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect((host, 8080))
    clientsocket1.send(bytes(f"INIT@{args.candidate_id}@", "utf-8"))


    # clientsocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # clientsocket2.connect((host, 8081))
    # clientsocket2.send(bytes(f"INIT@{args.candidate_id}@", "utf-8"))



    internal_state = StateManager(args.candidate_id, args.cluster)
    election_manager = ElectionManager(internal_state, clientsocket1)
    thread = threading.Thread(target=handle, args=(clientsocket1, election_manager, request_queue))
    thread.start()

    # thread = threading.Thread(target=handle_client, args=(clientsocket1, election_manager, ))
    # thread.start()
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