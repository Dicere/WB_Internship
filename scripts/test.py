import transform
import data_loader
import pandas as pd
import re
import json

def read_config(file_path):
    with open(file_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data
config_file_path = './scripts/param_for_test.json'
config = read_config(config_file_path)

iterator = data_loader.Loader(config['path_for_data'],step=config['step'])
for value in iterator:
    pred,df_pred = transform.predict(value)
    print(pred)
    print(df_pred)