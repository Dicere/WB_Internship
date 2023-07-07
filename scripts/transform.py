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

config_file_path = './scripts/config_transform.json' # Путь до конфига
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
            step : int 
                Шаг чтения, если ничего не указано шаг будет равен количетсву строк в df (по умолчанию None).
        Методы
        ------
        def get_agg_data(row,ids,general_columns):
            Возвращает df к которому добавлены данные из df_agg.
        def transform(row):
            Используется для преобразования данных.
    """
    def __init__(self,col_by_id3,pattern,list_colmn_calc,df_agg=df_agg):
        self.col_by_id3 = col_by_id3 # название колонок которые будем использовать из df_agg
        self.df_agg = df_agg #df с данными сгруппированными по id3
        self.list_id3_df_agg = self.df_agg['id3'] # список id3 в df_agg
        self.pattern = pattern # паттерн по которому мы будем искать все f фичи в df
        self.list_colmn_id3 = [re.search(pattern, item).group(0) for item in self.col_by_id3]
        self.list_colmn_calc = list_colmn_calc # название колонок которые мы получаем путём сложения или умножения и так далее
        self.calc_dict = {} # словарь в котором будет лежать key: название операции (умножение,...), value: лист с f, где слева f которая используется в левой части выражения, правая используется справа
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
        """
        Метод который производит первый этап обработки. Сначала вычитает множества чтобы получить список id3,
        которых ещё нету в df_agg, то что уже есть в df_agg присоединяется к нашим данным. После, те id3 которых не было в df_agg
        заполняються соответсвующими значениями из столбцов f1-f8.
        Возвращает df с добавленными данными
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
        Первый этап добавить данные из df_agg через метод get_agg_data(). Второй этап необходимо выполнить
        различные действия между столбцами (умножение,...). Сначала формируется словарь calc_dict.
        Потом в цикле проходим по списку операций, получаем соотвествующие столбцы из наших данных и выполняем над ними операции.
        Создаём столбцы и заполняем их получеными значениями
        Метод возвращает трансформированный df
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
        for conversions in self.calc_dict.keys():
            for iter in range(0,len(self.calc_dict[conversions]),2):
                row[self.list_colmn_calc[i]]=self.conversions_list[conversions](row,self.calc_dict[conversions][iter],self.calc_dict[conversions][iter+1])
                i+=1
        row.replace([np.inf, -np.inf], 0, inplace=True)
        return row
    
def predict(value):
    """
    Метод для получения предикта, создаём объект класса Transform_data.
    Трансформируем данные, подгружаем модель.
    Делаем предикт на данных которые мы преобразовали используя подобранный порог.
    Возвращает вектор предиктов, и df с id1,id2,id3 и прогнозом 
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