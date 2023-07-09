import pandas as pd
from catboost import CatBoostClassifier,Pool
import numpy as np
import re
import json

def read_config(file_path: str):
    """
    Чтение json конфига

    Параметры
    ------
    file_path : str
        Полный путь до файла конфига.ы
    """
    with open(file_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data

config_file_path = './scripts/configs/config_transform.json' # Путь до конфига
config = read_config(config_file_path)

df_agg = pd.read_csv(config['path_df_agg'],index_col=[0]) # df в котором данные сгруппированные по id3 по которым посчитаны различные статистики

class Transform_data:
    """Класс Transform_data используется для преобразования df

        Основное применение - преобразование df для того чтобы потом делать предсказание

        Атрибуты
        ------
        col_by_id3: 
            Название колонок которые будем использовать из df_agg.
        df_agg: 
            Df с данными сгруппированными по id3.
        list_id3_df_agg: 
            Cписок id3 в df_agg.
        pattern : str 
            Паттерн по которому мы будем искать все f признаки в df.
        list_colmn_id3: 
            Список f признаков из col_by_id3.
        list_colmn_calc: 
            Список признаков которые потом будут получены путём арифимитечксих операций над ними.
        calc_dict: 
            Словарь в котором будет лежать key: название операции (умножение,...), value: лист с f признаками.
        conversions_list: 
            Список арифметических действий.
        Методы
        ------
        def def summ(row,firs_f,second_f):
            Возвращает сумму между столбцами.
        def subtraction(row,firs_f,second_f):
            Возвращает разность между столбцами.
        def multiplication(row,firs_f,second_f):
            Возвращает произведение между столбцами.
        def division(row,firs_f,second_f):
            Возвращает частное между столбцами.
        def get_agg_data(row,ids,general_columns):
            Возвращает df к которому добавлены данные из df_agg.
        def transform(row):
            Используется для преобразования данных.
    """
    def __init__(self,col_by_id3,pattern:str,list_colmn_calc,df_agg=df_agg):
        """Inits Loader.
        
        Параметры
        ------
        col_by_id3: 
            Название колонок которые будем использовать из df_agg.
        pattern : str 
            Паттерн по которому мы будем искать все f признаки в df.
        list_colmn_id3: 
            Список f признаков из col_by_id3.
        list_colmn_calc: 
            Список признаков которые потом будут получены путём арифимитечксих операций над ними.
        df_agg: 
            Df с данными сгруппированными по id3.
        """
        self.col_by_id3 = col_by_id3
        self.df_agg = df_agg 
        self.list_id3_df_agg = self.df_agg['id3'] 
        self.pattern = pattern 
        self.list_colmn_id3 = [re.search(pattern, item).group(0) for item in self.col_by_id3]
        self.list_colmn_calc = list_colmn_calc
        self.calc_dict = {}
        self.conversions_list = ['sum','subtraction','division','multiplication']

    def conversions(self,conversion,row,firs_f,second_f):
        """
        Метод summ возвращает сумму значений в столбцах.

        Параметры
        ---------
        conversions : 
            Тип операции.
        row : 
            Блок данных.
        firs_f:
            f признак, в левой части выражения.
        second_f:
            f признак, в правой части выражения.

        Возвращаемое значение
        ---------------------
        Результат операции.
        """
        if conversion == 'sum':
            return row.loc[:,firs_f] + row.loc[:,second_f]
        if conversion == 'subtraction':
            return row.loc[:,firs_f] - row.loc[:,second_f]
        if conversion == 'multiplication':
            return row.loc[:,firs_f] * row.loc[:,second_f]
        if conversion == 'division':
            return row.loc[:,firs_f] / row.loc[:,second_f]
        
    
    def get_agg_data(self,row,ids,general_columns):
        """
        Метод summ возвращает сумму значений в столбцах.

        Параметры
        ---------
        row : 
            Блок данных.
        ids:
            Список id3 из row.
        general_columns:
            Список колонок которые должны быть получены в результате присоединения данных.

        Возвращаемое значение
        ---------------------
        Df с добавленными данными.
        """
        set1 = set(self.list_id3_df_agg)
        set2 = set(ids)
        missing_ids = list(set2 - set1)
        df_merged = pd.merge(row, self.df_agg,left_on='id3', right_on='id3', how='left')
        df_merged.loc[df_merged['id3'].isin(missing_ids), self.col_by_id3] = row.loc[row['id3'].isin(missing_ids), self.list_colmn_id3].values
        return df_merged[general_columns]


    
    def transform(self,row):
        """
        Метод, который трансформирует данные для того чтобы потом сделать предикт.

        Параметры
        ---------
        row : 
            Блок данных.
        ids:
            Список id3 из row.
        general_columns:
            Список колонок которые должны быть получены в результате присоединения данных.

        Возвращаемое значение
        ---------------------
        Преобразованный блок данных.
        """
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
        for cur_conversion in self.calc_dict.keys():
            for iter in range(0,len(self.calc_dict[cur_conversion]),2):
                row[self.list_colmn_calc[i]]=self.conversions(cur_conversion,row,self.calc_dict[cur_conversion][iter],self.calc_dict[cur_conversion][iter+1])
                i+=1
        row.replace([np.inf, -np.inf], 0, inplace=True)
        return row
    
def predict(value):
    """
    Метод для получения предикта.

    Параметры
    ---------
    value : 
        Блок данных.

    Возвращаемое значение
    ---------------------
    pred_marks:
        Вектор прогнозов.
    pred_marks:
        Df c прогнозами.

    """
    transformer = Transform_data(config['col_id3'],config['pattern'],config['col_calculated'],df_agg)
    data = transformer.transform(value)
    model = CatBoostClassifier()
    model.load_model(config['path_model_cat'])
    pred_marks = (model.predict_proba(data)[:, 1]> config['threshold']).astype('int')
    df_result = pd.DataFrame({'id1': value['id1'],
            'id2': value['id2'],
            'id3': value['id3'],
            'Label': pred_marks})
    return pred_marks,df_result

if __name__ == "__main__":
    """
    Код проверяет работоспособность, просто запустить ( python .\scripts\transform.py )
    """
    import data_loader
    transformer = Transform_data(config['col_id3'],config['pattern'],config['col_calculated'],df_agg)
    iterator = data_loader.Loader(config['path_for_data'],step=config['step'])
    for value in iterator:
        pred,df_pred = predict(value)
        print(pred)
        print(df_pred)