import pandas as pd

class Loader:
    """Класс Loader используется для симуляции передачи потока данных

        Основное применение - передача входного файла .csv частями или полностью

        Атрибуты
        ------
            df: 
                Датафрейм по которому будем итерироваться и возвращать значения.
            num_rows: 
                Количество строк в df.
            current_row: 
                Текущая строка.
            step : int 
                Шаг чтения, если ничего не указано шаг будет равен количетсву строк в df (по умолчанию None).
        Методы
        ------
        __iter__():
            Возвращает итерируемы объект.
        __next__():
            Метод __next__ вовращает следующий блок данных из df.
    """
    def __init__(self,path:str,step=None):
        """Inits Loader.
        
        Параметры
        ------
        path : str
            Полный путь до файла.
        step : int
            Количество строк который будет возвращатся.
        Raises
        ------ 
        Exception
            Если файл не удалось счиать вызовется исключение.
        """
        try:
            self.df = pd.read_csv(path, index_col=[0],lineterminator='\n') 
        except pd.errors.ParserError as e:
            raise ValueError(f"Error parsing CSV file: {e}")
        self.num_rows = len(self.df) 
        self.current_row = 0 
        self.step = step if step is not None else len(self.df)
    
    def __iter__(self):
        """Возвращает сам объект Loader, чтобы он мог использоваться в качестве итератора."""
        return self
    
    def __next__(self):
        """
        Метод __next__ возвращает следующий блок данных из df.
        
        Возвращаемое значение
        ---------------------
        row : Блок данных.
        """
        if self.current_row >= self.num_rows: 
            raise StopIteration

        start_row = self.current_row
        end_row = min(self.current_row + self.step, len(self.df))
        rows = self.df.iloc[start_row:end_row]
        self.current_row += self.step
        
        return rows
    
if __name__ == "__main__":
    """
    Для проверки работоспособности, просто запустить ( python .\scripts\data_loader.py )
    """
    import json

    def read_config(file_path:str):
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

    config_file_path = './scripts/param_for_test.json' # Путь до конфига
    config = read_config(config_file_path)

    iterator = Loader(config['path_for_data'],step=config['step'])
    for value in iterator:
        print(value)