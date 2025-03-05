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
from utils import txt_to_object, object_to_txt
from data.models.reply_vote import ReplyVote
from data.models.append_entries import AppendEntries, AppendEntriesReply
from client import Transaction
from collections import deque
from data.models.request_vote import RequestVote





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
                if append_entries.transaction.type == "intra_shard":
                    x, y = append_entries.transaction.x, append_entries.transaction.y

                    if election_manager.state_manager.lock_table[x] != None or election_manager.state_manager.lock_table[y] != None:
                        continue
                    else:
                        election_manager.state_manager.lock_table[x] = append_entries.transaction.client_id
                        election_manager.state_manager.lock_table[y] = append_entries.transaction.client_id

                elif append_entries.transaction.type == "cross_shard":

                    x, y = append_entries.transaction.x, append_entries.transaction.y

                    if x != 0:
                        if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.data_manager.get_balance(x) >= append_entries.transaction.amount:
                            election_manager.state_manager.lock_table[x] = append_entries.transaction.client_id
                        else:
                            client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, transaction=append_entries.transaction, prepare_phase=False))}|@", "utf-8"))
                            
                
                    elif y != 0:
                        if election_manager.state_manager.lock_table[y] == None:

                            election_manager.state_manager.lock_table[y] = append_entries.transaction.client_id
                        else:
                            client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries, transaction=append_entries.transaction, prepare_phase=False))}|@", "utf-8"))




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
                    election_manager.state_manager.log_entries = election_manager.state_manager.log_entries[:append_entries.prev_log_ind + i]  # Delete conflicting entries
                    break


            for j in range(i, len(append_entries.log_entires)):
                election_manager.state_manager.log_entries.append(append_entries.log_entires[j])  # Append new entries

            last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()

            election_manager.state_manager.commit_index = max(election_manager.state_manager.commit_index, min(append_entries.commit_ind, append_entries.prev_log_ind + len(append_entries.log_entires)))
                # apply_committed_entries()
            election_manager.state_manager.persist()
            election_manager.reset_timer()
            election_manager.state_manager.apply_committed_entries()
            
            client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, True, candidate_id, append_entries, transaction=append_entries.transaction))}|@", "utf-8"))                    



            append_entries_queue.popleft()



def handle_client_thread(client_queue):

    while True:

        while len(client_queue) != 0:
            election_manager, piggy_back_obj = client_queue[0]

            transaction: Transaction = txt_to_object(piggy_back_obj)
            print(transaction)
            x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

            if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.lock_table[y] == None:

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
                

def handle_argument(request_queue, client_queue, append_entries_queue):

    while True:

        while len(request_queue) != 0:

            message, client, election_manager, piggy_back_obj = request_queue[0]

            if message == "COMMIT":
                print("Received Commit Message")
                transaction: Transaction = txt_to_object(piggy_back_obj)
                is_committed = election_manager.state_manager.apply_cross_shard_entries(transaction.client_id)
                if is_committed == True:
                    print("Sent Commit Message")
                    client.send(bytes(f"CLIENT_RELAY_ACK@{transaction.client_id}#ACK_SUCCESS@", "utf-8"))
                    

            elif message == "ABORT":

                print("Abort Received")
                
                transaction: Transaction = txt_to_object(piggy_back_obj)
                x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

                if x != 0:
                    election_manager.state_manager.lock_table[x] = None
                elif y != 0:
                    election_manager.state_manager.lock_table[y] = None

                print("Sending Ack")
                client.send(bytes(f"CLIENT_RELAY_ACK@{transaction.client_id}#ACK_FAIL@", "utf-8"))
                


            elif message == "PREPARE":
                if election_manager.leader_id == None:
                    pass 
                    # what to do when there is no leader
                
                if election_manager.leader_id != election_manager.state_manager.candidate_id:
                    new_message = f"PREPARE|{piggy_back_obj}"
                    election_manager.send_to(new_message, election_manager.leader_id)
                    request_queue.popleft()
                    continue
                
                print("Received Prepare message")
                transaction: Transaction = txt_to_object(piggy_back_obj)
                print(transaction)
                x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

                if x != 0:
                    print("PREPARE", client_id, election_manager.state_manager.lock_table[x])

                    if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.data_manager.get_balance(x) >= amount:

                        election_manager.state_manager.lock_table[x] = client_id
                    else:
                        print("Sent Client relay x")
                        client.send(bytes(f"CLIENT_RELAY@{transaction.client_id}#PREPARE_FAIL@", "utf-8"))
                        request_queue.popleft()
                        continue

                
                elif y != 0:
                    if election_manager.state_manager.lock_table[y] == None:

                        election_manager.state_manager.lock_table[y] = client_id
                    else:
                        print("Sent Cient Relay y")
                        client.send(bytes(f"CLIENT_RELAY@{transaction.client_id}#PREPARE_FAIL@", "utf-8"))
                        request_queue.popleft()
                        continue

                # message, piggy_back_obj = message.split("|")
                if len(election_manager.state_manager.log_entries) == 0:
                    election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, 1, transaction))
                else:
                    election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, election_manager.state_manager.log_entries[-1].index + 1, transaction))
                    
                election_manager.append_entries(transaction)


            elif message == "CLIENT":

                if election_manager.leader_id == None:
                    
                    print(f"leader id is None: {election_manager.leader_id}")
                    pass 
                    # what to do when there is no leader
                
                if election_manager.leader_id != election_manager.state_manager.candidate_id:
                    new_message = f"CLIENT|{piggy_back_obj}"
                    election_manager.send_to(new_message, election_manager.leader_id)
                    request_queue.popleft()
                    continue

                print("here", piggy_back_obj)
                transaction: Transaction = txt_to_object(piggy_back_obj)
                x, y, amount, client_id = transaction.x, transaction.y, transaction.amount, transaction.client_id

                print(client_id, election_manager.state_manager.lock_table[x])
                if election_manager.state_manager.data_manager.get_balance(x) >= amount:
                    if election_manager.state_manager.lock_table[x] == None and election_manager.state_manager.lock_table[y] == None:

                        election_manager.state_manager.lock_table[x] = client_id
                        election_manager.state_manager.lock_table[y] = client_id


                        print("Received client message:")

                        # message, piggy_back_obj = message.split("|")
                        if len(election_manager.state_manager.log_entries) == 0:
                            election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, 1, transaction))
                        else:
                            election_manager.state_manager.log_entries.append(LogEntry(election_manager.state_manager.current_term, election_manager.state_manager.log_entries[-1].index + 1, transaction))
                        
                        election_manager.state_manager.persist()
                        election_manager.append_entries(transaction)

                    else:
                        client_queue.append([election_manager, piggy_back_obj])
                else:
                    client.send(bytes(f"CLIENT_RELAY@{transaction.client_id}#INSUFFICIENT_FUNDS@", "utf-8"))


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
                request_vote: RequestVote = txt_to_object(piggy_back_obj)
                current_term = election_manager.state_manager.current_term
                voted_for = election_manager.state_manager.voted_for
                candidate_id = election_manager.state_manager.candidate_id

                if current_term == request_vote.term and (voted_for == candidate_id or voted_for == None):
                    # time.sleep(1)
                    client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(True, current_term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                    continue
                    
                if request_vote.term < current_term:
                    # time.sleep(1)
                    client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(False, current_term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                    continue
                

                if request_vote.term > current_term:
                    election_manager.state_manager.voted_for = None
                    election_manager.state_manager.current_term = request_vote.term
                    election_manager.state_manager.persist()

                    if election_manager.state_manager.state != State.FOLLOWER:
                        election_manager.switch_states(State.FOLLOWER)
                        election_manager.reset_timer()
                    
                
                current_term = election_manager.state_manager.current_term
                voted_for = election_manager.state_manager.voted_for
                candidate_id = election_manager.state_manager.candidate_id

                election_manager.leader_id = None
                last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()

                if (last_log_term > request_vote.last_log_term) or (last_log_term == request_vote.last_log_term and last_log_ind > request_vote.last_log_ind):
                    # time.sleep(1)
                    client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(True, request_vote.term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                    continue
                
                # time.sleep(1)
                client.send(bytes(f"RELAY@{request_vote.candidateId}#{"REPLY_VOTE|"}{object_to_txt(ReplyVote(True, request_vote.term, election_manager.state_manager.candidate_id))}|@", "utf-8"))
                print("Sent")
                
                election_manager.state_manager.voted_for = request_vote.candidateId
                election_manager.reset_timer()
                election_manager.state_manager.persist()

            elif message == "REPLY_VOTE":
                print(message)
                reply_vote: ReplyVote = txt_to_object(piggy_back_obj)
                print(reply_vote.vote, reply_vote.term, election_manager.votes_collected, reply_vote.candidate_id)

                if reply_vote.vote:
                    election_manager.votes_collected += 1
                    if election_manager.votes_collected >= election_manager.majority:
                        election_manager.switch_states(State.LEADER)
                else:
                    if election_manager.state_manager.current_term < reply_vote.term:
                        election_manager.switch_states(State.FOLLOWER)
                        election_manager.state_manager.current_term = reply_vote.term
                        election_manager.state_manager.voted_for = None
                        election_manager.leader_id = None
                        election_manager.state_manager.persist()
                        election_manager.reset_timer()
            
            elif message == "APPEND_ENTRIES":

                append_entries: AppendEntries = txt_to_object(piggy_back_obj)

                if append_entries.transaction != None:
                    append_entries_queue.append([message, client, election_manager, piggy_back_obj])
                else:

                    current_term = election_manager.state_manager.current_term
                    candidate_id = election_manager.state_manager.candidate_id
                    election_manager.leader_id = append_entries.leader_id

                    if current_term > append_entries.term:
                        client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, False, candidate_id, append_entries))}|@", "utf-8"))
                        # DPrintf("Error, Peer, I : %d DONOT write to log from master: %d, I HAVE BIG TERM MISMATCH: %d \n", rf.me, args.LeaderId, reply.Term)
                        print("Error 1")
                        continue
                


                    if append_entries.term > current_term:
                        election_manager.state_manager.current_term = append_entries.term
                        election_manager.state_manager.voted_for = None
                        # rf.persist() -> Persist to disk
                        election_manager.state_manager.persist()

                    print("Heartbeat Received")
                    print(append_entries.log_entires)
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
                    
                    i = 0
                    for i in range(len(append_entries.log_entires)):
                        if append_entries.prev_log_ind + i >= last_log_ind + 1:
                            break

                        if append_entries.prev_log_ind + i < len(election_manager.state_manager.log_entries) and election_manager.state_manager.log_entries[append_entries.prev_log_ind + i].term != append_entries.log_entires[i].term:
                            election_manager.state_manager.log_entries = election_manager.state_manager.log_entries[:append_entries.prev_log_index + i]  # Delete conflicting entries
                            break

                    last_client_id = election_manager.state_manager.get_last_log_client_id()



                    for j in range(i, len(append_entries.log_entires)):
                        if last_client_id == None or last_client_id != append_entries.log_entires[j].command.client_id:
                        # if election_manager.state_manager.lock_table[append_entries.log_entires[i].command.x] != None and election_manager.state_manager.lock_table[append_entries.log_entires[i].command.y] != None:
                            election_manager.state_manager.log_entries.append(append_entries.log_entires[j])  # Append new entries



                    last_log_ind, last_log_term = election_manager.state_manager.get_last_log_index_term()
                    election_manager.state_manager.commit_index = max(election_manager.state_manager.commit_index, min(append_entries.commit_ind, append_entries.prev_log_ind + len(append_entries.log_entires)))
                        # apply_committed_entries()
                    election_manager.state_manager.persist()
                    election_manager.reset_timer()
                    election_manager.state_manager.apply_committed_entries()
                    
                    client.send(bytes(f"RELAY@{append_entries.leader_id}#{"APPEND_REPLY|"}{object_to_txt(AppendEntriesReply(current_term, True, candidate_id, append_entries))}|@", "utf-8"))                    
    
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
                        print(f"Updated Next Index and Match Index")


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
                            if append_entry_reply.transaction != None:
                                
                                if append_entry_reply.transaction.type == "cross_shard":
                                    election_manager.future_commit_ind = prev_log_index + log_entries_len
                                    election_manager.state_manager.persist()
                                    # if append_entry_reply.transaction.x != 0:
                                    #     election_manager.state_manager.lock_table[append_entry_reply.transaction.x] = None
                                    # elif append_entry_reply.transaction.y != 0:
                                    #     election_manager.state_manager.lock_table[append_entry_reply.transaction.y] = None
                                    print("Sending prepare success")
                                    client.send(bytes(f"CLIENT_RELAY@{append_entry_reply.transaction.client_id}#PREPARE_SUCCESS@", "utf-8"))

                                else:
                                    election_manager.state_manager.commit_index = prev_log_index + log_entries_len
                                    election_manager.state_manager.persist()
                                    election_manager.state_manager.apply_committed_entries()
                                    # print(f"Debug: Leader {self.me} updated commit index to {self.commit_index}")
                                    # self.persist()
                    
                                    # self.notify_apply()
                                    client.send(bytes(f"CLIENT_RELAY@{append_entry_reply.transaction.client_id}#COMMIT_SUCCESS@", "utf-8"))
                        else:
                            print("Fail Case")
                            pass
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

                        if append_entry_reply.prepare_phase != None and append_entry_reply.prepare_phase == False:
                            client.send(bytes(f"CLIENT_RELAY@{append_entry_reply.transaction.client_id}#PREPARE_FAIL@", "utf-8"))
                        else:
                            # Follower log is inconsistent, decrement nextIndex
                            election_manager.next_ind[append_entry_reply.candidate_id] = max(1, min(append_entry_reply.conflict_ind, len(election_manager.state_manager.log_entries)))
                            # self.next_index[peer_id] = max(1, min(append_entry_reply.conflict_index, len(self.log)))
                    # print(f"Error: Leader {self.me}, peer {peer_id} is not up to date")

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


def run_server(args):
    host = 'localhost'  # Listen on the local machine only
    port = args.port  # Choose a port number

    request_queue = deque([])
    client_queue = deque([])
    append_entries_queue = deque([])


    thread = threading.Thread(target=handle_argument, args=(request_queue, client_queue, append_entries_queue, ))
    thread.start()

    thread = threading.Thread(target=handle_append_entries, args=(append_entries_queue, ))
    thread.start()


    thread = threading.Thread(target=handle_client_thread, args=(client_queue, ))
    thread.start()


    clientsocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket1.connect((host, args.port))
    clientsocket1.send(bytes(f"INIT@{args.candidate_id}@", "utf-8"))


    internal_state = StateManager(args.candidate_id, args.cluster)
    election_manager = ElectionManager(internal_state, clientsocket1, )


    thread = threading.Thread(target=handle, args=(clientsocket1, election_manager, request_queue))
    thread.start()

    thread = threading.Thread(target=election_manager.start_timer)
    thread.start()

    thread = threading.Thread(target=election_manager.leader_loop)
    thread.start()


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