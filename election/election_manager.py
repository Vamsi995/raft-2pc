from election.state_manager import State, StateManager
import threading
import random
import time
from data.models.request_vote import RequestVote
from utils import object_to_txt
from data.models.append_entries import AppendEntries
from election.data_manager import CSVUpdater

class ElectionManager:

    def __init__(self, state_manager: StateManager, comm_factory):
        self.state_manager = state_manager
        self.election_timeout = random.uniform(0.15, 0.3)  # Randomized timeout
        self.heartbeat_interval = 0.1  # Leader sends heartbeats every 100ms
        self.timer = None
        self.comm_factory = comm_factory
        self.start_time = None
        self.majority = 2
        self.votes_collected = 0
        self.is_timer_running = True
        self.leader_alive = False
        self.next_ind = {}
        self.leader_id = None
        self.match_ind = {}
        if self.state_manager.cluster_id == 1:
            self.candidates = set(list(range(1, 4)))
        elif self.state_manager.cluster_id == 2:
            self.candidates = set(list(range(4, 7)))
        elif self.state_manager.cluster_id == 3:
            self.candidates = set(list(range(7, 10)))
        self.candidates.remove(self.state_manager.candidate_id)
        self.future_commit_ind = None

        
    def init_next_ind(self):

        for k in self.candidates:
            last_log_index, _ = self.state_manager.get_last_log_index_term()
            self.next_ind[k] = last_log_index + 1
            self.match_ind[k] = 0
        # self.reset_election_timer()


    def leader_loop(self):

        time_limit = 5  # Timeout after 5 seconds
        start_time = time.time()
        self.leader_id = self.state_manager.candidate_id
        flag = 0 

        while True:
            while self.leader_alive:
                if flag == 0:
                    print("Started Leader Loop")
                flag = 1
                elapsed_time = time.time() - start_time
                if elapsed_time > time_limit:
                    self.append_entries()
                    start_time = time.time()
                    # self.reset_timer()
                    print("Append Entries sent!")
                
                time.sleep(5)


    def append_entries(self, transaction=None):

        for candidate_id, log_ind in self.next_ind.items():


            prev_log_index = log_ind - 1
            if prev_log_index > 0:
                prev_log_term = self.state_manager.log_entries[prev_log_index - 1].term
            else:
                prev_log_term = 0
            last_log_ind, last_log_term = self.state_manager.get_last_log_index_term()
            # print(last_log_ind, last_log_term, log_ind)

            if last_log_ind >= log_ind:
                log_entries = self.state_manager.log_entries[prev_log_index:]
            else:
                log_entries = []
            # print(log_entries)
            message = AppendEntries(self.state_manager.current_term, self.state_manager.candidate_id, prev_log_index, prev_log_term, log_entries, self.state_manager.commit_index, transaction) # Just send the locks required

            
            message = "APPEND_ENTRIES|" + object_to_txt(message)
            self.send_to(message, candidate_id)


    
    def switch_states(self, state):

        if self.state_manager.state == State.CANDIDATE:
            if state == State.LEADER:
                # print("I am the leader")
                self.cancel_timer()
                self.state_manager.state = state

                self.leader_alive = True
                self.leader_id = self.state_manager.candidate_id
                self.init_next_ind()
                # start periodic heartbeats

                # thread.join()
    

                # leader setup
                # cancel timer
                pass
            elif state == State.FOLLOWER:
                self.state_manager.switch_states(state)

        elif self.state_manager.state == State.LEADER:
            if state == State.LEADER:
                pass
        
            elif state == State.FOLLOWER:
                print("Stopped Leader Loop")
                self.leader_alive = False
                print("Started Election Timer")
                self.is_timer_running = True
        
        elif self.state_manager.state == State.FOLLOWER:
            if state == State.FOLLOWER:
                pass

    def broadcast(self, message):
        self.comm_factory.send(bytes(f"BROADCAST@{message}|@", "utf-8"))

    def send_to(self, message, candidate_num):
        self.comm_factory.send(bytes(f"RELAY@{candidate_num}#{message}|@", "utf-8"))

    def start_timer(self):
        """Starts a new election if no heartbeat received."""
        #Example usage
        time_limit = random.uniform(10, 20)  # Timeout after 5 seconds
        self.start_time = time.time()

        while True:
            while self.is_timer_running:
                elapsed_time = time.time() - self.start_time
                if elapsed_time > time_limit:
                    print("Timeout occurred!")
                    self.start_election()
                    # self.reset_timer()
                time.sleep(2)


    
    def reset_timer(self):
        if self.start_time != None:
            self.start_time = time.time()
    
    def cancel_timer(self):
        self.is_timer_running = False

    def start_election(self):
        print("Election Started!")
        if self.state_manager.state == State.FOLLOWER:
            
            self.state_manager.switch_states(State.CANDIDATE)
            # Add getter and setter for this -> these need to be stored on the disk
            self.state_manager.current_term += 1
            self.state_manager.voted_for = self.state_manager.candidate_id

            self.votes_collected += 1

            last_log_index = self.state_manager.log_entries[-1].index if len(self.state_manager.log_entries) > 0 else 0
            last_log_term = self.state_manager.log_entries[-1].term if len(self.state_manager.log_entries) > 0 else 0

            # broadcast request votes
            request_vote = RequestVote(self.state_manager.candidate_id, self.state_manager.current_term, last_log_index, last_log_term)
            message = "REQUEST_VOTE|"+object_to_txt(request_vote)
            self.state_manager.persist()
            self.broadcast(message)
            self.reset_timer()

        elif self.state_manager.current_term == State.CANDIDATE:
            
            # increment term
            self.state_manager.current_term += 1
            self.state_manager.voted_for = self.state_manager.candidate_id

            self.votes_collected += 1
            
            last_log_index = self.state_manager.log_entries[-1].index if len(self.state_manager.log_entries) > 0 else 0
            last_log_term = self.state_manager.log_entries[-1].term if len(self.state_manager.log_entries) > 0 else 0

            # broadcast request votes
            request_vote = RequestVote(self.state_manager.candidate_id, self.state_manager.current_term, last_log_index, last_log_term)
            message = "REQUEST_VOTE|"+object_to_txt(request_vote)
            self.state_manager.persist()
            self.broadcast(message)
            self.reset_timer()

            # restart election
            # broadcast request votes

            

        # print(f"Node {self.node_id}: Election timeout! Becoming candidate and starting election")
        # self.reset_election_timer()  # Reset timeout to avoid multiple elections

    def receive_heartbeat(self):
        """Follower receives heartbeat from leader, preventing election."""
        if self.state == "follower":
            print(f"Node {self.node_id}: Received heartbeat, resetting election timeout")
            self.reset_election_timer()
