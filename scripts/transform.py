import data_loader
import pandas as pd
from catboost import CatBoostClassifier,Pool
import numpy as np
import random
import re
import json

def read_config(file_path):
    with open(file_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

config_file_path = './scripts/config_transform.json'
config = read_config(config_file_path)

df_agg = pd.read_csv(config['path_df_agg'],index_col=[0])

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
    
    def get_agg_data(self,row,ids,general_columns):
        set1 = set(self.list_id3_df_agg)
        set2 = set(ids)
        missing_ids = list(set2 - set1)
        df_merged = pd.merge(row, self.df_agg,left_on='id3', right_on='id3', how='left')
        df_merged.loc[df_merged['id3'].isin(missing_ids), self.col_by_id3] = row.loc[row['id3'].isin(missing_ids), self.list_colmn_id3].values
        return df_merged[general_columns]


    
    def transform(self,row):
        columns_strat_agg = list(row.drop('label', axis=1).columns[4::]) + self.col_by_id3
        ids = row['id3']
        row = self.get_agg_data(row,ids,columns_strat_agg)
        i=0
        if self.calc_dict == {}:
            for item in self.list_colmn_calc:
                parts = item.split('_')
                key = parts[0]
                values = [parts[1], parts[2]]
                if key in self.calc_dict:
                    self.calc_dict[key].extend(values)
                else:
                    self.calc_dict[key] = values
        for conversions in self.calc_dict.keys():
            for iter in range(0,len(self.calc_dict[conversions]),2):
                row[self.list_colmn_calc[i]]=self.conversions_list[conversions](row,self.calc_dict[conversions][iter],self.calc_dict[conversions][iter+1])
                i+=1
        row.replace([np.inf, -np.inf], 0, inplace=True)
        return row
    
def predict(value):
    transformer = Transform_data(config['col_id3'],config['pattern'],config['col_calculated'],df_agg)
    data = transformer.transform(value)
    model = CatBoostClassifier()
    model.load_model(config['path_model_cat'])
    pred_marks = (model.predict_proba(data)[:, 1]> config['threshold']).astype('int')
    df_result = pd.DataFrame({'id1': value['id1'],
            'id2': value['id2'],
            'id3': value['id3'],
            'Label': pred_marks})
    # df_result.to_csv(config['path_for_save'])
    return pred_marks,df_result

# iterator = data_loader.Loader(config['path_for_data'],step=config['step'])
# transformer = Transform_data(config['col_id3'],config['pattern'],config['col_calculated'],df_agg)



# for value in iterator:
#     data = transformer.transform(value)
#     pred_marks = (model.predict_proba(data)[:, 1]> config['threshold']).astype('int')
#     df_result = pd.DataFrame({'id1': value['id1'],
#                 'id2': value['id2'],
#                 'id3': value['id3'],
#                 'Label': pred_marks})
#     df_result.to_csv(config['path_for_save'])
#     print(pred_marks)