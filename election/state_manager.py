from enum import Enum
import json

class State(Enum):
    LEADER = 1
    FOLLOWER = 1
    CANDIDATE = 1

class LogEntry:

    def __init__(self, term, index, command):
        self.term = term
        self.index = index
        self.command = command


class StateManager:

    def __init__(self, candidate_id: int):
        self.state = State.FOLLOWER
        self.candidate_id = candidate_id


        self.filename = None

        # These need to be stored on disk
        self.current_term = 0
        self.voted_for = None
        self.log_entries = []

    # @property
    # def current_term(self):
    #     with open(self.filename, "r") as file:
    #         data = json.load(file)
    #     return data['currentTerm']
    
    # @current_term.setter
    # def current_term(self, new_val):
    #     self.current_term = new_val
    #     with open(self.filename, "w") as file:
    #         json.dump(self.to_dict(), file, indent=4)


    def to_dict(self):
        """Convert instance variables to a dictionary."""
        return {
            "currentTerm": self.current_term,
            "votedFor": self.voted_for,
            "log_entries": self.log_entries
        }

    def save_to_file(self, filename):
        """Save the instance variables to a file in JSON format."""
        with open(filename, "w") as file:
            json.dump(self.to_dict(), file, indent=4)
        print(f"Data saved to {filename}.")


    def load_from_file(self, filename):
        """Create an instance of Person by reading variables from a JSON file."""
        with open(filename, "r") as file:
            data = json.load(file)
        print(f"Data loaded from {filename}.")

        self.current_term, self.voted_for, self.log_entries = data["currentTerm"], data["voted_for"], data["log_entries"])

    def switch_states(self, state):
        self.state = state


    