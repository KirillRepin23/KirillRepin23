# Настоящий скрипт выгружает данные из FraudScore о фродовых установках со статусом фрода HIGH

# Библиотеки
import requests
import sys
import logging
import pandas as pd
from datetime import date, timedelta
import json
from google.oauth2 import service_account

'''Блок логгирования'''

#file_log = logging.FileHandler('load_prec_csv_YA.log')
console_out = logging.StreamHandler()

logging.basicConfig(handlers=[console_out],
                    level=logging.DEBUG, 
                    format = "%(funcName)s: %(lineno)d - %(message)s")

'''Блок рабочей дирректории'''

main_path = 'C:/Users/user1882/RW_projects/Holodilnik/Holodilnik_scripts_airflow/'

# Credentials для BQ
credentials = service_account.Credentials.from_service_account_file(main_path + 'holodilnik-bq-376311-50cc6f8cfc32.json')

# Начальная и конечная даты выгрузки данных из FraudScore
today = date.today().strftime('%Y-%m-%d')
load_date = (date.today()-timedelta(days=2)).strftime('%Y-%m-%d')

# Инициализация параметров запроса и запрос к FraudScore
CHANNEL = 'ho*******************'
USER_KEY = '12***********************'
EVENT = 'install'
url = f'https://get.fraudscore.ai/{CHANNEL}/{EVENT}/groups'
params = dict(key = USER_KEY)
params['field'] = ['datetime','affiliate_name', 'offer_name','id_2', 'score_str'] #поле id_2 содержит уникальный appmetrica_device_id пользователя
params['sort'] = [
	'datetime_asc']
params['filter'] = json.dumps([
	"AND",
	["date_gt", load_date],
	["date_lt", today]
  ])
params['page_size'] = 1000000
r = requests.get(url, params, stream = True)

# Формирование Pandas DataFrame
datetime1 = []
offer_name = []
affiliate_name = []
id_2 = []
score_str = []

for line in r.iter_lines():
 data = json.loads(line)
 datetime1 += [data['datetime']]
 offer_name += [data['offer_name']]
 affiliate_name += [data['affiliate_name']]
 id_2 += [data['id_2']]
 score_str += [data['score_str']]
  
d = {'datetime1': datetime1, 'offer_name': offer_name, 'affiliate_name': affiliate_name, 'id_2': id_2, 'score': score_str}
df_fraud_score = pd.DataFrame(data=d)

if df_fraud_score.shape[0] == 0:
   logging.info('Нулевой датафрейм')
   sys.exit(1)
else:
   ''' Выбор (при необходимости) фродовых установок по статусу фрода high. Лучше выгружать данные по всем установкам, которые ловит FraudScore, 
    чтобы оставалась возможность анализировать другие категории фрода и проверять качество работы FraudScore
   '''
   df_fraud_score_high = df_fraud_score.query("score == 'high'")

# Загрузка DF в BQ 
df_fraud_score_high.to_gbq('holodilnik-bq-376311.appmetrica_dashboard.high_fraudscore_appmetrica_device_id', 
          project_id='holodilnik-bq-376311',  
          if_exists='append',
          credentials=credentials)

# Загружаем таблицу обратно и исключаем возможные дубли
sql = """SELECT * FROM `holodilnik-bq-376311.appmetrica_dashboard.high_fraudscore_appmetrica_device_id`"""
all_data = pd.read_gbq(sql, credentials=credentials, dialect='standard')
all_data = all_data.drop_duplicates()

# Загружаем итоговую таблицу в BQ с замещением существующей
all_data.to_gbq('holodilnik-bq-376311.appmetrica_dashboard.high_fraudscore_appmetrica_device_id', 
           project_id='holodilnik-bq-376311',  
           if_exists='replace',
           credentials=credentials)