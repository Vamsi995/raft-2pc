from state_manager import StateManager, State
import threading
import random
import time
from communication_factory import CommunicationFactory
from data.models.request_vote import RequestVote
from utils import object_to_txt

class ElectionManager:

    def __init__(self, state_manager: StateManager, comm_factory: CommunicationFactory):
        self.state_manager = state_manager
        self.election_timeout = random.uniform(0.15, 0.3)  # Randomized timeout
        self.heartbeat_interval = 0.1  # Leader sends heartbeats every 100ms
        self.timer = None
        self.comm_factory = comm_factory

        self.reset_election_timer()

    def reset_election_timer(self):
        """Resets the election timer with a new randomized timeout."""
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(self.election_timeout, self.start_election)
        self.timer.start()
        print(f"Node {self.node_id}: Reset election timeout to {self.election_timeout:.3f} seconds")

    def start_election(self):
        """Starts a new election if no heartbeat received."""

        if self.state_manager.state == State.FOLLOWER:

            self.state_manager.switch_states(State.CANDIDATE)

            # Add getter and setter for this -> these need to be stored on the disk
            self.state_manager.current_term += 1
            self.state_manager.voted_for = self.state_manager.candidate_id

            # broadcast request votes
            request_vote = RequestVote(self.state_manager.candidate_id, self.state_manager.current_term, self.state_manager.log_entries[-1].index, self.state_manager.log_entries[-1].term)
            message = "REQUEST_VOTE|"+object_to_txt(request_vote)
            self.comm_factory.broadcast(message)

        elif self.state_manager.current_term == State.CANDIDATE:
            
            # increment term
            self.state_manager.current_term += 1
            self.state_manager.voted_for = self.state_manager.candidate_id
            
            request_vote = RequestVote(self.state_manager.candidate_id, self.state_manager.current_term, self.state_manager.log_entries[-1].index, self.state_manager.log_entries[-1].term)
            message = "REQUEST_VOTE|"+object_to_txt(request_vote)
            self.comm_factory.broadcast(message)

            # restart election
            # broadcast request votes

            

        print(f"Node {self.node_id}: Election timeout! Becoming candidate and starting election")
        self.reset_election_timer()  # Reset timeout to avoid multiple elections

    def receive_heartbeat(self):
        """Follower receives heartbeat from leader, preventing election."""
        if self.state == "follower":
            print(f"Node {self.node_id}: Received heartbeat, resetting election timeout")
            self.reset_election_timer()
