# Настоящий скрипт выгружает данные из Appmetrica о покупках в приложении

# Библиотеки
import requests
import sys
import logging
import time
import pandas as pd
from datetime import date, timedelta
import json
from google.oauth2 import service_account
import pandas_gbq

'''Блок логгирования'''

#file_log = logging.FileHandler('load_prec_csv_YA.log')
console_out = logging.StreamHandler()

logging.basicConfig(handlers=[console_out],
                    level=logging.DEBUG, 
                    format = "%(funcName)s: %(lineno)d - %(message)s")

'''Блок рабочей дирректории'''

main_path = 'C:/Users/user1882/RW_projects/Holodilnik/Holodilnik_scripts_airflow/'


def send_request(application_id: str,
                 key: str,
                 fields: str
                 ):

    ''' 
      Функция, в которой отправляется GET запрос к API Appmetrica (revenue_events) с требованием 
      прислать отчет за вчерашний день по запрашиваемым параметрам

      Для использования API Appmetrica необходимо получить авторизационный токен через OAuth-сервер Яндекса.
      Токен необходимо передавать для каждого метода в HTTP-заголовке Authorization.
      типо Authorization: OAuth "ваш_токен"

      На вход подаются аргументы:
      application_id: str -- идентификационный номер приложения Appmetrica
      key: str -- авторизационный токен
      fields: str - строка с перечислением запрашиваемых показателей

      Функция возвращает скаченный датасет в формате pd.DataFrame.
    '''

    # Начальная и конечная даты выгрузки данных из Appmetrica
    date_since = (date.today()-timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
   
    date_until = date.today().strftime('%Y-%m-%d %H:%M:%S')

    # Инициализация параметров запроса и запрос к Appmetrica
    url= f'https://api.appmetrica.yandex.ru/logs/v1/export/revenue_events.json?application_id={application_id}&date_since={date_since}&date_until={date_until}&fields={fields}'
    header = {'Authorization': key}
    response1 = requests.get(url=url, headers=header)
    logging.info(f'Запрос для: {url}')
    k=1
    while response1.status_code != 200 and k<100:
      response1 = requests.get(url=url, headers=header)
      logging.info(f'Запрос для: {url}')
      print(response1.status_code)
      time.sleep(5*k)
      k+=1

    if response1.status_code !=200: 
      logging.info('100 неуспешных попыток получить данные из Appmetrica')
      sys.exit(1)

    # Формирование Pandas DataFrame
    js1_df = response1.json()
    df1 = pd.DataFrame(js1_df['data'])
    if df1.shape[0] == 0:
      logging.info('Нулевой датафрейм')
      sys.exit(1)
    else:
      # Выбор установок c параметром is_reattribution=='false
      df1['full_revenue'] = df1['revenue_quantity'].astype('float') * df1['revenue_price'].astype('float')

    return df1



def write_to_bq(df: pd.DataFrame,
                 json_credentials_file_name: str,
                 bq_project_id: str,
                 bq_table_id: str
                 ):
    ''' 
      Функция, в которой отправляется запрос к API BQ с требованием 
      записать pandas DataFrame в указанную таблицу BQ

      Для использования API BQ необходимо активировать в Google Cloud Platform соответствующий функционал API и получить авторизационный токен в разделе Credentials.
      
      На вход подаются аргументы:
      df: pd.DataFrame -- pandas DataFrame, который хотим загрузить в BQ
      json_credentials_file_name: str -- имя JSON-файла с credentials проекта BQ
      bq_project_id: str -- проект BQ, куда записываем pandas DataFrame
      bq_table_id: str - таблица BQ, куда записываем pandas DataFrame

      Функция создает таблицу в BQ и записывает туда pandas DataFrame в таблицу BQ. 
      Если таблица BQ уже существует, то функция добавляет pandas DataFrame к существующей таблице с последующим удалением возможных дублей строк.
    '''

    # Credentials для BQ
    credentials = service_account.Credentials.from_service_account_file(main_path + json_credentials_file_name)

    # Загрузка DF в BQ 
    df.to_gbq(bq_table_id, 
            project_id= bq_project_id,  
            if_exists='append',
            credentials=credentials)

    # Загружаем таблицу обратно и исключаем возможные дубли
    sql = f"""SELECT * FROM {bq_table_id}"""
    all_data = pd.read_gbq(sql, credentials=credentials, dialect='standard')
    all_data = all_data.drop_duplicates()

    # Загружаем итоговую таблицу в BQ с замещением существующей
    all_data.to_gbq(bq_table_id, 
             project_id=bq_project_id,  
             if_exists='replace',
             credentials=credentials)


''' Блок выполнения функций'''

key = 'OAuth *******************************A'
fields = 'appmetrica_device_id,event_name,event_datetime,revenue_quantity,revenue_price,revenue_product_id,revenue_order_id,is_revenue_verified,device_manufacturer'
application_id = '*******'

df_installs_rec = send_request(application_id = application_id, key = key, fields = fields)


json_credentials = 'holodilnik-bq-376311-50cc6f8cfc32.json'
bq_project_id = 'holodilnik-bq-376311'
bq_table_id = 'holodilnik-bq-376311.appmetrica_dashboard.purchases'

write_to_bq(df=df_installs_rec,json_credentials_file_name= json_credentials,bq_project_id = bq_project_id,bq_table_id = bq_table_id)