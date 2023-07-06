import pandas as pd
import numpy as np
import random

class Loader:
    def __init__(self,path,step=None):
        try:
            self.df = pd.read_csv(path, index_col=[0],lineterminator='\n')
        except pd.errors.ParserError as e:
            raise ValueError(f"Error parsing CSV file: {e}")
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