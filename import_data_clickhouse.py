from urllib.parse import quote_plus

import os
import pandas
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = 'subjects' # название создаваемой таблицы в ClickHouse  
DATASOURCE = os.getenv('DATASOURCE')

# Данные для подключения к PostgreSQL

USER = os.getenv('USER_POSTGRES', default='postgres')
PASSWORD = os.getenv('PASSWORD_POSTGRES', default='default')
HOST = os.getenv('HOST_POSTGRES', default='localhost')
PORT = os.getenv('PORT_POSTGRES', default='5432')
DATABASE_NAME = os.getenv('DATABASE_NAME_POSTGRES', default='default')

# Параметры для подключения к clickhouse

connect = {
    'USER_CLICK' : os.getenv('USER_CLICK', default='default'),
    'PASSWORD_CLICK' : str(os.getenv('PASSWORD_CLICK', default='default')),
    'HOST_CLICK' : os.getenv('HOST_CLICK', default='localhost'),
    'PORT_CLICK' : os.getenv('PORT_CLICK', default='8123'),
    'DATASOURCE' : os.getenv('DATASOURCE')
}
# Подключение к PostgreSQL

engine_click = create_engine(
    'clickhouse://{USER_CLICK}:{PASSWORD_CLICK}@{HOST_CLICK}:{PORT_CLICK}/{DATASOURCE}'.format(**connect)
)
conn_click = engine_click.connect().execution_options(stream_results=True)

# Подключение к Сlickhouse

engine = create_engine(
    f'postgresql://{USER}:%s@{HOST}:{PORT}/{DATABASE_NAME}'% quote_plus(PASSWORD)
)
conn = engine.connect().execution_options(stream_results=True)

# Получение датафрейма из PostgreSQL

df = sqlalchemy.text(
    """
        SELECT *
        FROM  for_tests."subjects"
    """
) # пример

df = pandas.read_sql(df, conn)

# Получение названий колонок и их типов

columns = dict(df.dtypes)
query2 = str()

for key, val in columns.items():
    if val == 'object':
        val = 'string'
    if val in ('datetime64[ns]', 'timedelta[ns]',):
        val = 'datetime'
    query2 += f''' `{key}` {str(val).capitalize()}, '''
print(query2)

# Запрос clickhouse для создания таблицы

create_table_query = (
    f'''
        CREATE TABLE IF NOT EXISTS `{DATASOURCE}`.{TABLE_NAME} (
            {query2[:-2]}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
    '''
)

truncate_table = (
    f''' TRUNCATE TABLE IF EXISTS `{DATASOURCE}`.{TABLE_NAME} '''
)

# Создание таблицы в clickhouse и загрузка данных
engine_click.execute(create_table_query)
engine_click.execute(truncate_table)
df.to_sql(
    TABLE_NAME,
    con=engine_click,
    if_exists='append',
    index=False
)
