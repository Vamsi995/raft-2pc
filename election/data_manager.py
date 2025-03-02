import pandas as pd
from election.lock_table import LockTable

class CSVUpdater:
    def __init__(self, file_path, lock_table: LockTable):
        self.file_path = file_path
        self.lock_table = lock_table

    def update_cell(self, from_index, to_index, column_name, val):
        try:
            df = pd.read_csv(self.file_path)
            df.loc[df['id'] == from_index, column_name] -= val
            df.loc[df['id'] == to_index, column_name] += val
            df.to_csv(self.file_path, index=False)
            self.lock_table[from_index] = None
            self.lock_table[to_index] = None
        except Exception as e:
            print(f"Error processing CSV: {e}")
    
    def get_balance(self, from_index):
        df = pd.read_csv(self.file_path)
        return float(df.loc[df['id'] == from_index, 'balance'].iloc[0])
        # df.loc[df['id'] == to_index, column_name] += val
        # df.to_csv(self.file_path, index=False)
        



# if __name__ == "__main__":
#     df = pd.read_csv("/Users/vamsi/Documents/GitHub/raft-2pc/data/cluster1_server1_data.csv")

