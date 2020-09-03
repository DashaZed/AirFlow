import datetime
import json
import io
import logging
import os
import pandas_gbq
import pandas as pd
import requests
import time
import urllib.request, urllib.parse, urllib.error

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import AirflowException
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from requests.auth import HTTPBasicAuth


GCLOUD_JSON_KEY ='key.json' # Gcloud's JSON KEY
GCLOUD_PROJECT_ID = '' # Gcloud's PROJECT KEY
DATASET_BIGQUERY = 'appmetrica' # BigQuery's dataset name

bigquery_client = bigquery.Client.from_service_account_json(os.path.relpath(GCLOUD_JSON_KEY))
dataset_ref = bigquery_client.dataset(DATASET_BIGQUERY)


APPMETRICA_YAPASPORT_KEY = '' 
APPLICATION_ID = 0

APPMETRICA_FIELDS = {
    'table':
    'profiles',
    'fields': [
        'profile_id', 'appmetrica_gender', 'appmetrica_birth_date',
        'appmetrica_notifications_enabled', 'appmetrica_name',
        'appmetrica_crashes', 'appmetrica_errors',
        'appmetrica_first_session_date', 'appmetrica_last_start_date',
        'appmetrica_push_opens', 'appmetrica_push_send_count',
        'appmetrica_sdk_version', 'appmetrica_sessions', 'android_id',
        'appmetrica_device_id', 'city', 'connection_type', 'country_iso_code',
        'device_manufacturer', 'device_model', 'device_type', 'google_aid',
        'ios_ifa', 'ios_ifv', 'mcc', 'mnc', 'operator_name', 'os_name',
        'os_version', 'windows_aid', 'app_build_number', 'app_framework',
        'app_package_name', 'app_version_name'
    ]
}


def if_tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except:
        return False
    

def upload_to_bq(table_name, dataframe):

    table_id = GCLOUD_PROJECT_ID + '.' + DATASET_BIGQUERY + '.' + table_name

    schem = []
    data_df = dataframe.copy()

    data_df.columns=data_df.columns.str.replace('$','')
    data_df.columns=data_df.columns.str.replace('properties.','')
    
    dataset = bigquery_client.dataset('appmetrica')
    table_ref = dataset.table(table_name)
    
    data_df.fillna('0', inplace=True)
    
    data_df = data_df[['profile_id', 'appmetrica_gender', 'appmetrica_birth_date',
       'appmetrica_notifications_enabled', 'appmetrica_name',
       'appmetrica_crashes', 'appmetrica_errors',
       'appmetrica_first_session_date', 'appmetrica_last_start_date',
       'appmetrica_push_opens', 'appmetrica_push_send_count',
       'appmetrica_sdk_version', 'appmetrica_sessions', 'android_id',
       'appmetrica_device_id', 'city', 'connection_type', 'country_iso_code',
       'device_manufacturer', 'device_model', 'device_type', 'google_aid',
       'ios_ifa', 'ios_ifv', 'mcc', 'mnc', 'operator_name', 'os_name',
       'os_version', 'windows_aid', 'app_build_number', 'app_framework',
       'app_package_name', 'app_version_name']]
    

    for col in APPMETRICA_FIELDS['fields']:
        if col not in data_df:
            data_df[col] = '0'
        data_df[col] = data_df[col].astype(str)
 
    
    if if_tbl_exists(bigquery_client, table_ref):
  
        table = bigquery_client.get_table(table_ref)  # API Request
        schem = table.schema
        
        for column in data_df:
            data_df[column] = data_df[column].apply(lambda x: 'DICT' if isinstance(x,dict) else x)
            data_df[column] = data_df[column].apply(lambda x: 'LIST' if isinstance(x, list) else x)
            # If it's a nested field we drop it
            if not data_df.loc[data_df[column]=='DICT'].empty:
                data_df = data_df.drop([column],axis=1)
            elif not data_df.loc[data_df[column]=='LIST'].empty:
                data_df = data_df.drop([column],axis=1)
    else:
        for column in data_df:
            data_df[column] = data_df[column].apply(lambda x: 'DICT' if isinstance(x,dict) else x)
            data_df[column] = data_df[column].apply(lambda x: 'LIST' if isinstance(x, list) else x)
            # If it's a nested field we drop it
            if not data_df.loc[data_df[column]=='DICT'].empty:
                data_df = data_df.drop([column],axis=1)
            elif not data_df.loc[data_df[column]=='LIST'].empty:
                data_df = data_df.drop([column],axis=1)
            elif data_df[column].dtype == 'int64':
                schem.append(SchemaField(column,'INTEGER'))
            elif data_df[column].dtype == 'float64':
                schem.append(SchemaField(column,'FLOAT'))
            elif data_df[column].dtype == 'bool':
                schem.append(SchemaField(column,'BOOLEAN'))
            elif data_df[column].dtype == 'datetime64[ns]':
                schem.append(SchemaField(column,'INT64'))
            else:
                schem.append(SchemaField(column,'STRING'))

    job_config = bigquery.LoadJobConfig(schema=schem)

    job = bigquery_client.load_table_from_dataframe(data_df, table_id, job_config=job_config) 



def load_from_appm(*op_args, conf=None, **context):
    
    start_date = op_args[0]
    end_date = str(op_args[1])

    daterange = pd.date_range(start_date, end_date)

    for single_date in daterange:
        PARAMS = {'application_id': APPLICATION_ID ,
                'date_since': start_date ,
                'date_until': end_date ,
                'date_dimension': 'default',
                'use_utf8_bom': 'true',
                'fields': ','.join(APPMETRICA_FIELDS['fields']),
                'appmetrica_first_session_date': str(single_date.strftime("%Y-%m-%d"))}

        headers = {"Authorization": "OAuth "+ APPMETRICA_YAPASPORT_KEY}
        print("GET requests from appmetrica table: ", APPMETRICA_FIELDS['table'], str(single_date.strftime("%Y-%m-%d")))
        URL = 'https://api.appmetrica.yandex.ru/logs/v1/export/' + APPMETRICA_FIELDS['table'] + '.json?'
    
        r = requests.get(URL, params=PARAMS, headers=headers)
        timer=0
        if r.status_code != 200:
            while r.status_code != 200:
                if r.status_code in [400,500,403]:
                    print('Bad Code=', r.status_code, ' response text=', r.text, 'at=', datetime.datetime.now())
                    quit()
                time.sleep(10) 
                timer+=10
                print('awaiting ',timer,' seconds, resp.status_code', r.status_code)
                r = requests.get(URL, params=PARAMS, headers=headers)
        df = pd.read_json(bytes(r.text, 'utf-8'), orient='split')  # ['data']
        del r
        if not df.empty:
            print('data loaded from appmetrica')
            print('loading to BigQuery...')
            # LOADING TO BIGQUERY
            upload_to_bq('profiles', df)
            del df
        else:
            print('no data for loading to BigQuery')
    
    Variable.set("last_update_profiles_appmetrica", end_date)

    
def delete_duplicates(**op_kwargs):
    
    delete_duplicates_query = (
    '''CREATE OR REPLACE TABLE appmetrica.profiles AS
      SELECT ARRAY_AGG(t ORDER BY appmetrica_device_id, appmetrica_last_start_date DESC limit 1)  row
      FROM appmetrica.profiles t
      GROUP BY appmetrica_device_id 
        )   ''')  
    
    logging.info('delete_duplicates_query: ', delete_duplicates_query)

    query_job = bigquery_client.query(delete_duplicates_query) 



args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 9, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=10)
}


start_date =  datetime.datetime.strptime(Variable.get("last_update_profiles_appmetrica"),'%Y-%m-%dT00:00:00')
last_date = time.strftime("%Y-%m-%dT00:00:00")


with DAG('profiles_appmetrica',
         default_args=args,
         schedule_interval='@daily'
         ) as dag:
    
    
    get_profiles = PythonOperator(task_id='get_profiles',
                                        provide_context=True,
                                        python_callable=load_from_appm,
                                        op_args=[start_date, last_date])
    
    delete_duplicates = PythonOperator(task_id='delete_duplicates',
                                        provide_context=True,
                                        python_callable=delete_duplicates
                                        )

    get_profiles >> delete_duplicates