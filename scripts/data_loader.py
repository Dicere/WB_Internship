import pandas as pd
# from catboost import CatBoostClassifier,Pool
import numpy as np
import random

df_test = pd.read_csv('./scripts/data_for_test.csv',index_col=[0])
df_agg = pd.read_csv('./scripts/db_agg_by_id3.csv',index_col=[0])


class Loader:
    def __init__(self, df=df_test,df_agg=df_agg,step=None):
        self.df = df
        self.df_agg = df_agg
        self.num_rows = len(df)
        self.current_row = 0
        self.step = step if step is not None else len(df)
    
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
    
    # def get_agg_data(self,id):
        
    #     if list(id) in list(self.df_agg['id3']):
    #         return self.df_agg[self.df_agg['id3']==list(id)]
    #     else:
    #         return pd.DataFrame()
    # def get_agg_data(self, ids):
    #     set1 = set(self.df_agg['id3'])
    #     set2 = set(ids)
    #     missing_ids = list(set2 - set1)

    #     # missing_df = pd.DataFrame({col: 0 for col in self.df_agg.columns if col != 'id3'}, index=missing_ids)
    #     df_merged = pd.merge(self.df, self.df_agg,left_on='id3', right_on='id3', how='left')

    #     print(df_merged)
        # missing_ids = set(ids) - set(filtered_df['id3'])
        
        # if len(missing_ids) > 0:
        #     missing_rows = pd.DataFrame({'id3': list(missing_ids)})
        #     empty_columns = self.df_agg.columns.drop('id3')
        #     for column in empty_columns:
        #         missing_rows[column] = None
            
        #     filtered_df = pd.concat([filtered_df, missing_rows], ignore_index=True)
        
        # return filtered_df

    
# iterator = Loader(step=10)

# # Итерируемся по значениям и печатаем их
# for value in iterator:
#     print(value)