from enum import Enum


class State(Enum):
    LEADER = 1
    FOLLOWER = 1
    CANDIDATE = 1


class StateManager:

    def __init__(self):
        self.state = State.FOLLOWER

    