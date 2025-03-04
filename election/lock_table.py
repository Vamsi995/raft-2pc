import pandas as pd

class LockTable:

    def __init__(self, cluster_id, candidate_id):

        self.cluster_id = cluster_id
        self.candidate_id = candidate_id
        self.lock_table = {}
        self.file_path = f"/Users/vamsi/Documents/GitHub/raft-2pc/data/cluster{self.cluster_id}_server{self.candidate_id}_data.csv"
        self.get_lock_ids()
    
    def get_lock_ids(self):

        df = pd.read_csv(self.file_path)
        ids = df['id'].tolist()
        self.lock_table = dict.fromkeys(ids)

    def __setitem__(self, key, value):
        self.lock_table[key] = value
    
    def __getitem__(self, key):
        return self.lock_table[key]