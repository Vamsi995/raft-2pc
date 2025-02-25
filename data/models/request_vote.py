class RequestVote:

    def __init__(self, candidateId, term, last_log_ind, last_log_term):
        self.candidateId = candidateId
        self.term = term
        self.last_log_ind = last_log_ind
        self.last_log_term = last_log_term
        