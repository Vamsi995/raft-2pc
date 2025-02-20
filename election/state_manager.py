from enum import Enum


class State(Enum):
    LEADER = 1
    FOLLOWER = 1
    CANDIDATE = 1


class StateManager:

    def __init__(self):
        self.state = State.FOLLOWER

        # These need to be stored on disk
        self.current_term = 0
        self.voted_for = None
        self.log_entries = []


    