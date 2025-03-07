from enum import Enum
import json
from election.data_manager import CSVUpdater
import os
from election.lock_table import LockTable

class State(Enum):
    LEADER = 1
    FOLLOWER = 2
    CANDIDATE = 3


class Transaction:


    def __init__(self, x, y, amount, type, client_id=None):
        self.x = x
        self.y = y
        self.amount = amount
        self.type = type
        if client_id == None:
            self.client_id = id(self)
        else:
            self.client_id = client_id
    
    def to_dict(self):
        return {
            'x': self.x,
            'y': self.y,
            'amount': self.amount,
            'type': self.type,
            'client_id': self.client_id
        }

    @classmethod
    def from_dict(cls, dict_obj):
        return cls(dict_obj['x'], dict_obj['y'], dict_obj['amount'], dict_obj['type'], dict_obj['client_id'])
    
    def __repr__(self):
        return f"Transaction[{self.x}, {self.y}, {self.amount}]"


class LogEntry:

    def __init__(self, term, index, command: Transaction):
        self.term = term
        self.index = index
        self.command = command
    
    def __repr__(self):
        return f"Term: {self.term}, Index: {self.index}, Command: {self.command}"

    def to_dict(self):
        return {
            'term': self.term,
            'index': self.index,
            'command': self.command.to_dict()
        }

    @classmethod
    def from_dict(cls, dict_obj):
        return cls(dict_obj['term'], dict_obj['index'], Transaction.from_dict(dict_obj['command']))

class StateManager:

    def __init__(self, candidate_id: int, cluster_id: int, ):
        self.state = State.FOLLOWER
        self.candidate_id = candidate_id
        self.commit_index = 0
        self.last_cross_shard = 0
        self.filename = None
        # These need to be stored on disk
        self.current_term = 0
        self.voted_for = None
        self.log_entries = []
        self.cluster_id = cluster_id
        self.lock_table = LockTable(self.cluster_id, self.candidate_id)
        self.data_manager = CSVUpdater(f"/Users/vamsi/Documents/GitHub/raft-2pc/data/cluster{self.cluster_id}_server{self.candidate_id}_data.csv", self.lock_table)
        self.meta_data_file = f"/Users/vamsi/Documents/GitHub/raft-2pc/data/metadata/c{self.cluster_id}s{self.candidate_id}_metadata.json"
        self.last_applied = len(self.log_entries)
        self.load_from_file()



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
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log_entries": [x.to_dict() for x in self.log_entries],
            "last_applied": self.last_applied,
            "last_cross_shard_applied": self.last_cross_shard
        }

    def persist(self):
        # print(self.log_entries)
        """Save the instance variables to a file in JSON format."""
        with open(self.meta_data_file, "w") as file:
            json.dump(self.to_dict(), file, indent=4)
        # print(f"Data saved to {self.meta_data_file}.")


    def load_from_file(self):
        """Create an instance of Person by reading variables from a JSON file."""
        if os.stat(self.meta_data_file).st_size == 0:
            self.persist()
            return
        with open(self.meta_data_file, "r") as file:
            data = json.load(file)
        print(f"Data loaded from {self.meta_data_file}.")

        self.current_term, self.voted_for, log_entries, self.last_applied, self.last_cross_shard = data["current_term"], data["voted_for"], data["log_entries"], data["last_applied"], data["last_cross_shard_applied"]
        self.log_entries = []
        for log in log_entries:
            self.log_entries.append(LogEntry.from_dict(log))


    def switch_states(self, state):
        self.state = state
    
    def get_last_log_index_term(self):
        last_log_index = self.log_entries[-1].index if len(self.log_entries) > 0 else 0
        last_log_term = self.log_entries[-1].term if len(self.log_entries) > 0 else 0
        return last_log_index, last_log_term

    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            entry: LogEntry = self.log_entries[self.last_applied]
            transaction: Transaction = entry.command
            if transaction.type == "intra_shard":
                self.data_manager.update_cell(transaction.x, transaction.y, 'balance', transaction.amount)
            # key, value = entry.command.split("=")
            # self.state_machine[key.strip()] = value.strip()
            self.last_applied += 1
            # print(f"Node {self.node_id}: Applied {entry.command} to state machine")
        self.persist()
    
    def apply_cross_shard_entries(self, client_id):
        
        while self.last_cross_shard < len(self.log_entries):
            entry: LogEntry = self.log_entries[self.last_cross_shard]
            transaction: Transaction = entry.command
            if transaction.type == "cross_shard" and transaction.client_id == client_id:
                self.data_manager.update_cross_shard_cell(transaction.x, transaction.y, 'balance', transaction.amount)
                self.last_cross_shard += 1
                self.persist()
                return True
            
            self.last_cross_shard += 1
        
        self.persist()
        return False

    def get_last_log_client_id(self):
        return self.log_entries[-1].command.client_id if len(self.log_entries) > 0 else None

