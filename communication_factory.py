from __future__ import annotations
import threading
import logging
import time
from election.timeout_manager import TimeoutManager
from election.state_manager import StateManager, State
from utils import txt_to_object, object_to_txt
from data.models.request_vote import RequestVote
from data.models.reply_vote import ReplyVote

class CommunicationFactory:

    REPLIES = []
    CLIENTS = []


    def broadcast(self, message):
        # time.sleep(3)
        for client in self.CLIENTS:
            client.send(bytes(message, "utf-8"))
        
        # logging.info(f"[Event - Broadcast - {message_type}] - [Clock - {lamport_clock.logical_time}] - [Sent from Client {lamport_clock.proc_id}]")


    def receive(self, server, client_limit, election_manager):
        while True:
            # Accept Connection
            client, address = server.accept()
            print("Connected with {}".format(client.getpeername()))
            self.CLIENTS.append(client)

            # Start Handling Thread For Client
            thread = threading.Thread(target=self.handle, args=(client, election_manager))
            thread.start()

            if len(self.CLIENTS) == client_limit:
                break


    def handle(self, client, election_manager):

        # If term > currentTerm, currentTerm ← term
        # (step down if leader or candidate)
        # 2. If term == currentTerm, votedFor is null or candidateId,
        # and candidate's log is at least as complete as local log,
        # grant vote and reset election timeout

        while True:
            try:
                # Broadcasting Messages
                message = client.recv(4096).decode("utf-8")
                message, piggy_back_obj = message.split("|")

                if message == "REPLY_VOTE":
                    reply_vote: ReplyVote = txt_to_object(piggy_back_obj)
                    election_manager.comm_factory.REPLIES.append(reply_vote.vote)
                    

                if election_manager.state_manager.internal_state.state == State.FOLLOWER:
                    if message == "HEARTBEAT":
                        election_manager.reset_election_timer()
                    elif message == "REQUEST_VOTE":
                        request_vote: RequestVote = txt_to_object(piggy_back_obj)
                        if request_vote.term > election_manager.state_manager.currentTerm:
                            election_manager.state_manager.currentTerm = request_vote.term
                        
                        if request_vote.term == election_manager.state_manager.currentTerm and (election_manager.state_manager.voted_for == None or election_manager.state_manager.voted_for == election_manager.state_manager.candidate_id):
                            if request_vote.last_log_term < election_manager.state_manager.last_log_term:
                                # deny vote
                                reply = "REPLY_VOTE" + "|" + object_to_txt(ReplyVote(False, election_manager.state_manager.currentTerm))
                                client.send(bytes(reply, "utf-8"))
                            elif request_vote.last_log_term > election_manager.state_manager.last_log_term:
                                # vote granted
                                reply = "REPLY_VOTE" + "|" + object_to_txt(ReplyVote(True, election_manager.state_manager.currentTerm))
                                client.send(bytes(reply, "utf-8"))
                            else:
                                if request_vote.last_log_ind < election_manager.state_manager.last_log_ind:
                                    # deny vote
                                    reply = "REPLY_VOTE" + "|" + object_to_txt(ReplyVote(False, election_manager.state_manager.currentTerm))
                                    client.send(bytes(reply, "utf-8"))
                                else:
                                    # grant vote
                                    reply = "REPLY_VOTE" + "|" + object_to_txt(ReplyVote(True, election_manager.state_manager.currentTerm))
                                    client.send(bytes(reply, "utf-8"))
                                    
                

                elif election_manager.state_manager.internal_state.state == State.CANDIDATE:
                    if message == "REQUEST_VOTE":
                         

                # if message == "REQUEST":
                #     attached_clock = txt_to_object(piggy_back_obj)
                #     lamport_clock.update_clock(attached_clock.logical_time)
                #     logging.info(f"[Event - REQUEST] - [Clock - {lamport_clock.logical_time}] - [Received from Client {attached_clock.proc_id}]")

                #     # Add to local queue
                #     lamport_clock()
                #     reply_message = "REPLY" + "|" + object_to_txt(lamport_clock)
                #     time.sleep(3)
                #     client.send(bytes(reply_message, "utf-8"))
                #     pqueue.insert(lamport_clock)
                #     logging.info(f"[Event - REPLY] - [Clock - {lamport_clock.logical_time}] - [Sent from Client {lamport_clock.proc_id}]")


                # elif message == "REPLY":
                #     attached_clock = txt_to_object(piggy_back_obj)
                #     comm_factory.REPLIES.append(client)
                #     lamport_clock.update_clock(attached_clock.logical_time)
                #     logging.info(f"[Event - REPLY] - [Clock - {lamport_clock.logical_time}] - [Received from Client {attached_clock.proc_id}]")


                # elif message == "RELEASE":
                #     attached_clock = txt_to_object(piggy_back_obj)
                #     lamport_clock.update_clock(attached_clock.logical_time)
                #     pqueue.delete(attached_clock.proc_id)
                #     logging.info(f"[Event - RELEASE] - [Clock - {lamport_clock.logical_time}] - [Received from Client {attached_clock.proc_id}]")
                #     client_interface.update_balance()

                # elif message == "BLOCK":
                #     piggy_back_clock, piggy_back_block = piggy_back_obj.split("#")
                #     attached_clock = txt_to_object(piggy_back_clock)
                #     block: Block = txt_to_object(piggy_back_block)
                #     lamport_clock.update_clock(attached_clock.logical_time)
                #     block_chain.update_head(block)
                #     balance_table[int(block.sender)] -= block.amount
                #     balance_table[int(block.receiver)] += block.amount
                #     logging.info(f"[Event - BLOCK] - [Clock - {lamport_clock.logical_time}] - [Received from Client {attached_clock.proc_id}]")


                    
            except Exception as e:
                print(e)
                # Removing And Closing Clients
                CommunicationFactory.CLIENTS.remove(client)
                client.close()
                break

