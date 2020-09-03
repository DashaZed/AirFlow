import logging
import json
import requests
import pandas as pd
import datetime
import urllib.request, urllib.parse, urllib.error
import time
import pytz
import io
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import AirflowException
from google.oauth2 import service_account
from mixpanel_api import Mixpanel
from pandas.io.json import json_normalize 
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from dateutil.relativedelta import relativedelta


VERSION = '2.0'  # Mixpanel API version
date_format = '%Y-%m-%d'  # Mixpanel's API date format
MIXPANEL_API_KEY = ''  # Mixpanel's API KEY
MIXPANEL_API_SECRET = '' # Mixpanel's API SECRET KEY

keys = (MIXPANEL_API_KEY, MIXPANEL_API_SECRET)

GCLOUD_JSON_KEY ='key.json' # Gcloud's JSON KEY
GCLOUD_PROJECT_ID = '' # Gcloud's PROJECT KEY
DATASET_BIGQUERY = 'mixpanel' # BigQuery's dataset name

bigquery_client = bigquery.Client.from_service_account_json(os.path.relpath(GCLOUD_JSON_KEY))
dataset_ref = bigquery_client.dataset(DATASET_BIGQUERY)


users_columns = [  
    '$distinct_id', '$ae_first_app_open_date', 'last_seen',
    '$ae_total_app_session_length', '$ae_total_app_sessions', '$city',
    '$country_code', '$geo_source', '$region','events_loaded'
]


def if_tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False

def upload_to_bq(table_name, dataframe):

    table_id = GCLOUD_PROJECT_ID + '.' + DATASET_BIGQUERY + '.' + table_name
    table_ref = dataset_ref.table(table_name)

    schem = []
    data_df = dataframe.copy()
    data_df['events_loaded']= False

    for col in users_columns:         
        if col == 'ae_total_app_session_length' or col == 'ae_total_app_sessions' or col == 'events_loaded':
            pass
        else:
            data_df[col] = data_df[col].astype(str) 

    data_df.columns=data_df.columns.str.replace('$','')
    data_df.columns=data_df.columns.str.replace('properties.','')
    
    if if_tbl_exists(bigquery_client, table_ref):  
        table = bigquery_client.get_table(table_ref)  # API Request
        schem = table.schema
    else:
        for column in data_df:
            data_df[column] = data_df[column].apply(lambda x: 'DICT' if isinstance(x, dict) else x)
            data_df[column] = data_df[column].apply(lambda x: 'LIST' if isinstance(x, list) else x)
            # If it's a nested field we drop it
            if not data_df.loc[data_df[column] == 'DICT'].empty:
                data_df = data_df.drop([column], axis=1)
            elif not data_df.loc[data_df[column] == 'LIST'].empty:
                data_df = data_df.drop([column],axis=1)
            elif data_df[column].dtype == 'int64':
                schem.append(SchemaField(column, 'INTEGER'))
            elif data_df[column].dtype == 'float64':
                schem.append(SchemaField(column, 'FLOAT'))
            elif data_df[column].dtype == 'bool':
                schem.append(SchemaField(column, 'BOOLEAN'))
            elif data_df[column].dtype == 'datetime64[ns]':
                schem.append(SchemaField(column, 'INT64'))
            else:
                schem.append(SchemaField(column,'STRING'))

    job_config = bigquery.LoadJobConfig(schema=schem)

    job = bigquery_client.load_table_from_dataframe(data_df, table_id, job_config=job_config)        


def update_users(*op_args, conf=None, **context):
    
    logging.warning('This is a context message')
    logging.warning(context)

    users = pd.DataFrame(columns=users_columns)
    
    start_date = op_args[0]
    end_date = str(op_args[1])
    
    logging.warning(start_date)

    logging.warning(end_date)

    
    m = Mixpanel(MIXPANEL_API_SECRET)
    
    selector = '((properties["$last_seen"] >= "'+start_date+'") and (properties["$last_seen"] < "'+end_date+'") and (defined (properties["$ios_app_version"]))) or ((properties["$ae_first_app_open_date"] >= "'+start_date+'") and (properties["$ae_first_app_open_date"] < "'+end_date+'") and (defined (properties["$ios_app_version"])))'
        
    parameters = { 'selector' : selector }

    m.export_people('people_export_ios.json', params=parameters, timezone_offset=None, format='json', compress=False)

    people_export = pd.read_json('people_export.json')

    people_export_df = pd.concat([people_export.drop(['$properties'], axis=1), people_export['$properties'].apply(pd.Series)],                                     axis=1)
    
    

    users = users.append(people_export_df)
    
    logging.warning(users.head(2))
    users['events_loaded'] = False
    
    upload_to_bq('users', users)
    
    end_date_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
    now = datetime.datetime.strptime(time.strftime("%Y-%m-%dT01:00:00"),'%Y-%m-%dT01:00:00')  
    
    
    if end_date_date <= now: 
        Variable.set("last_update_users", last_date)
    else:
        return None
    
    return str(conf)


def delete_duplicates(**op_kwargs):
    
    delete_duplicates_query = (
    '''CREATE OR REPLACE TABLE mixpanel.users AS
        SELECT row.*
        FROM (
        SELECT ARRAY_AGG(t ORDER BY last_seen DESC limit 1)[OFFSET(0)] row
        FROM mixpanel.users t
        GROUP BY distinct_id
            )''')  
    
    logging.info('delete_duplicates_query: ', delete_duplicates_query)

    query_job = bigquery_client.query(delete_duplicates_query) 
    
    
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 8, 13),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=30),
}

first_date = Variable.get("last_update_users")
last_date = datetime.datetime.strptime(Variable.get("last_update_users"),'%Y-%m-%dT00:00:00')
last_date = last_date + datetime.timedelta(days=1)
last_date = last_date.strftime('%Y-%m-%dT00:00:00')


with DAG('users',
         default_args=args,
         schedule_interval='@daily'
         ) as dag:
    
    
    update_users = PythonOperator(task_id='update_users',
                                        provide_context=True,
                                        python_callable=update_users,
                                        op_args=[first_date, last_date])
    
    delete_duplicates = PythonOperator(task_id='delete_duplicates',
                                        provide_context=True,
                                        python_callable=delete_duplicates
                                        )

    update_users >> delete_duplicates
    