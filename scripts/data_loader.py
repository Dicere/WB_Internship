import pandas as pd
import numpy as np
import random

df_test = pd.read_csv('./scripts/data_for_test.csv',index_col=[0])
df_agg = pd.read_csv('./scripts/db_agg_by_id3.csv',index_col=[0])


class Loader:
    def __init__(self,path,df_agg=df_agg,step=None):
        self.df = pd.read_csv(path,index_col=[0])
        self.df_agg = df_agg
        self.num_rows = len(self.df)
        self.current_row = 0
        self.step = step if step is not None else len(self.df)
    
    def __iter__(self):
        return self
    
    def __next__(self):
        
        if self.current_row >= self.num_rows:
            raise StopIteration

        start_row = self.current_row
        end_row = min(self.current_row + self.step, len(self.df))
        rows = self.df.iloc[start_row:end_row]
        self.current_row += self.step
        
        return rows