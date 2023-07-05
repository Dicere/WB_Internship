import data_loader
import pandas as pd
from catboost import CatBoostClassifier,Pool
import numpy as np
import random
import re

df_agg = pd.read_csv('./db_agg_by_id3.csv',index_col=[0])

kvad_sum=(lambda x: sum(i**2 for i in x))
kvad_sum.__name__ = 'kvad_sum'

interkvartil_razmah = (lambda x: np.percentile(x, 75) - np.percentile(x, 25))
interkvartil_razmah.__name__ = 'interkvartil_razmah'

range_dannih = (lambda x: x.max()-x.min())
range_dannih.__name__ = 'range_dannih'

quant90 = lambda x: x.quantile(0.9)
quant90.__name__ = 'quant90'

class Transform_data:
    def __init__(self,col_by_id3,pattern,list_colmn_calc,df_agg=df_agg):
        self.col_by_id3 = col_by_id3
        self.df_agg = df_agg
        self.list_id3_df_agg = self.df_agg['id3']
        self.pattern = pattern
        self.list_colmn_id3 = [re.search(pattern, item).group(0) for item in self.col_by_id3]
        self.list_colmn_calc = list_colmn_calc
        self.calc_dict = {}
        self.conversions_list = {'sum':self.summ,'subtraction':self.subtraction,'division':self.division,'multiplication':self.multiplication}

    def summ(self,row,firs_f,second_f):
        return row.loc[:,firs_f] + row.loc[:,second_f]
    def subtraction(self,row,firs_f,second_f):
        return row.loc[:,firs_f] - row.loc[:,second_f]
    def multiplication(self,row,firs_f,second_f):
        return row.loc[:,firs_f] * row.loc[:,second_f]
    def division(self,row,firs_f,second_f):
        return row.loc[:,firs_f] / row.loc[:,second_f]
    
    def get_agg_data(self,row,ids):
        set1 = set(self.list_id3_df_agg)
        set2 = set(ids)
        missing_ids = list(set2 - set1)
        current_ids = set2-set(missing_ids)
        df_merged = pd.merge(row, self.df_agg,left_on='id3', right_on='id3', how='left')
        print(df_merged)
        # df_merged = 


    
    def transform(self,row):
        ids = row['id3']
        self.get_agg_data(row,ids)
        row =row[row.drop('label', axis=1).columns[4::]]
        i=0
        
        # if agg_row.empty:
        #     dict_values = dict(zip(self.col_by_id3,row[self.list_colmn_id3].values.astype(list)[0]))
        #     row = row.assign(**dict_values)
        # else:
        #     row.loc[:,self.col_by_id3] = agg_row[self.col_by_id3].values
        # if self.calc_dict == {}:
        #     for item in self.list_colmn_calc:
        #         parts = item.split('_')
        #         key = parts[0]
        #         values = [parts[1], parts[2]]
        #         if key in self.calc_dict:
        #             self.calc_dict[key].extend(values)
        #         else:
        #             self.calc_dict[key] = values
        # for conversions in self.calc_dict.keys():
        #     for iter in range(0,len(self.calc_dict[conversions]),2):
        #         row[self.list_colmn_calc[i]]=self.conversions_list[conversions](row,self.calc_dict[conversions][iter],self.calc_dict[conversions][iter+1])
        #         i+=1
        # row.replace([np.inf, -np.inf], 0, inplace=True)
        # return row

col_id3 = ['f1_by_id3_median', 'f6_by_id3_interkvartil_razmah', 'f7_by_id3_mean']
col_calculated = ['sum_f3_f6', 'division_f1_f2', 'division_f4_f7', 'division_f7_f5', 'division_f7_f8', 'multiplication_f3_f3']
pattern = r'^f\d+'

iterator = data_loader.Loader(num_rows=50)
transformer = Transform_data(col_id3,pattern,col_calculated,df_agg)

for value in iterator:
    print(value)
    # data = transformer.transform(value)
    # print(data[col_id3])
    # break