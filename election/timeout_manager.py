import threading
import random
import time

class TimeoutManager:
    def __init__(self, internal_state):
        self.state = internal_state
        self.election_timeout = random.uniform(0.15, 0.3)  # Randomized timeout
        self.heartbeat_interval = 0.1  # Leader sends heartbeats every 100ms
        self.timer = None
        # self.reset_election_timer()

    def reset_election_timer(self):
        """Resets the election timer with a new randomized timeout."""
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(self.election_timeout, self.start_election)
        self.timer.start()
        print(f"Node {self.node_id}: Reset election timeout to {self.election_timeout:.3f} seconds")

    def start_election(self):
        """Starts a new election if no heartbeat received."""
        self.state = "candidate"
        print(f"Node {self.node_id}: Election timeout! Becoming candidate and starting election")
        self.reset_election_timer()  # Reset timeout to avoid multiple elections

    def receive_heartbeat(self):
        """Follower receives heartbeat from leader, preventing election."""
        if self.state == "follower":
            print(f"Node {self.node_id}: Received heartbeat, resetting election timeout")
            self.reset_election_timer()

# # Simulate a Raft node
# node = RaftNode(1)
# time.sleep(1)  # Simulating some passage of time before heartbeat
# node.receive_heartbeat()
