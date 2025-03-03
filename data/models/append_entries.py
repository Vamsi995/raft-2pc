class AppendEntries:

    def __init__(self, term, leader_id, prev_log_index, prev_log_term, log_entires, commit_ind, transaction):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_ind = prev_log_index
        self.prev_log_term = prev_log_term
        self.log_entires = log_entires
        self.commit_ind = commit_ind
        self.transaction = transaction


class AppendEntriesReply:

    def __init__(self, term, success, candidate_id, append_entries_request, conflict_ind=None, conflict_term=None, transaction=None):
        self.term = term 
        self.success = success
        self.candidate_id = candidate_id
        self.conflict_ind = conflict_ind
        self.conflict_term = conflict_term
        self.append_entries_request = append_entries_request
        self.transaction = transaction
        