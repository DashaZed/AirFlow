import json
import airflow
import requests
import pandas as pd
from datetime import timedelta, datetime

import datetime
import logging
import hashlib
import urllib.request, urllib.parse, urllib.error
import time
import pytz
import io
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException 
from google.cloud.exceptions import NotFound
from pandas.io.json import json_normalize 
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account


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


def read_events(keys, events=None, start=None, end=None, did=None,
                where=None, bucket=None, columns=None, exclude_mp=True):

    if start is None:
        start = datetime.date(2011, 0o7, 10)
        
    start = pd.to_datetime(start)
    start_str = start.strftime(date_format)

    if end is None:  # Defaults to yesterday, the latest allowed
        end = datetime.date.today() - datetime.timedelta(1)
    end = pd.to_datetime(end)
    end_str = end.strftime(date_format)

    payload = {
        'from_date' : start_str,
        'to_date' : end_str,
    }

    # Handle when single event passed in as string
    if isinstance(events, str):
        events = [events]

    params = {'event' : events, 'where' : where, 'bucket' : bucket}
    for k, v in params.items():
        if v is not None:
            payload[k] = v

    data = request(keys, ['export'], payload, data_api=True)
    
    return _export_to_df(data, columns, exclude_mp)

def to_df(data, columns, exclude_mp, did):
    events = []
    parameters = set()
    for line in data.split(b'\n'):
        try:
            event = json.loads(line)

            if event['properties']['distinct_id'] == did:
                ev = event['properties']
                ev['event']=event['event']
                parameters.update(list(ev.keys()))
                events.append(ev)
            else:
                continue

        except ValueError:  # Not valid JSON
            continue

    if columns is None:
        if exclude_mp:
            columns = [p for p in parameters if not (p.startswith('$') or
                                                     p.startswith('mp_'))]
        else:
            columns = parameters
    elif 'time' not in columns:
        columns.append('time')
    else:
        df['time'] = df['time'].map(lambda x: datetime.datetime.fromtimestamp(x))
  
    df = pd.DataFrame(events, columns=columns)

    return df


def _export_to_df(data, columns, exclude_mp):
    
    parameters = set()
    events = []
    for line in data.split(b'\n'):
        try:
            event = json.loads(line)
            ev = event['properties']
            ev['event']=event['event']
        except ValueError:  # Not valid JSON
            continue
        parameters.update(list(ev.keys()))
        events.append(ev)

    # If columns is excluded, leave off parameters that start with '$' as
    # these are automatically included in the Mixpanel events and clutter the
    # real data
    if columns is None:
        if exclude_mp:
            columns = [p for p in parameters if not (p.startswith('$') or
                                                     p.startswith('mp_'))]
        else:
            columns = parameters
            
    elif 'time' not in columns:
        columns.append('time')
    else:
        df['time'] = df['time'].map(lambda x: datetime.datetime.fromtimestamp(x))

        
    df = pd.DataFrame(events, columns=columns)

    return df

def request(keys, methods, params, format='json', data_api=False):
    """
        methods - List of methods to be joined, 
                      e.g. ['events', 'properties', 'values']
                  will give us 
                      http://mixpanel.com/api/2.0/events/properties/values/
        params - Extra parameters associated with method
    """
    api_key, api_secret = keys

    params['api_key'] = api_key
    params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.
    params['format'] = format
    if 'sig' in params: del params['sig']
    params['sig'] = hash_args(params, api_secret)

    if data_api:
        url_base = r'http://data.mixpanel.com/api'
    else:
        url_base = r'http://mixpanel.com/api'

    request_url = ('/'.join([url_base, str(VERSION)] + methods) + '/?' + 
                   unicode_urlencode(params))

    request = urllib.request.urlopen(request_url)
    data = request.read()

    if data_api:
        return data

    return json.loads(data)


def unicode_urlencode(params):
    """
        Convert lists to JSON encoded strings, and correctly handle any 
        unicode URL parameters.
    """
    if isinstance(params, dict):
        params = list(params.items())
    for i, param in enumerate(params):
        if isinstance(param[1], list): 
            params[i] = (param[0], json.dumps(param[1]),)

    return urllib.parse.urlencode(
        [(k, isinstance(v, str) and v.encode('utf-8') or v) 
            for k, v in params]
    )


def hash_args(args, api_secret):
    """
        Hashes arguments by joining key=value pairs, appending a secret, and 
        then taking the MD5 hex digest.
    """
    for a in args:
        if isinstance(args[a], list): args[a] = json.dumps(args[a])

    args_joined = b''
    for a in sorted(args.keys()):
        if isinstance(a, str):
            args_joined += a.encode('utf-8')
        else:
            args_joined += str(a).encode('utf-8')

        args_joined += b'='

        if isinstance(args[a], str):
            args_joined += args[a].encode('utf-8')
        else:
            args_joined += str(args[a]).encode('utf-8')

    hash = hashlib.md5(args_joined)

    hash.update(api_secret.encode('utf-8'))
    return hash.hexdigest()  

    
def change_events_fl(**op_kwargs):
    
    c_dids = op_kwargs["num"]

    logging.info('CDIDS: ', c_dids)

    dml_statement = (
    "UPDATE mixpanel.users SET events_loaded = True WHERE distinct_id IN "+str(tuple(c_dids))+" ")  
    
    logging.info('dml_statement: ', dml_statement)

    query_job = bigquery_client.query(dml_statement) 
    

def get_events(**op_kwargs):
    
    events = pd.DataFrame(columns=events_columns)
    
    start_date = op_kwargs["start"]
    end_date = op_kwargs["end"]
    c_dids = op_kwargs["num"]
    logging.warning(len(c_dids))
        
    date_first = start_date[:start_date.find('T')]
    date_first = datetime.datetime.strptime(date_first, "%Y-%m-%d").date()
    
    end_date = end_date[:end_date.find('T')]
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    
    logging.warning(date_first)
    logging.warning(end_date)
        
    where = ''
    
    for i in c_dids:
        where += 'properties["$distinct_id"] == "'+i+'" or '
        
    logging.warning(where)

    events_df = read_events(keys, events=events_to_use, start=date_first, end=end_date, where=where[:-4])

    events = events.append(events_df)

    upload_to_bq('events', events)
    
    
def if_tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False
    

def upload_to_bq(table_name, dataframe):

    table_id = GCLOUD_PROJECT_ID + '.' + DATASET_BIGQUERY + '.' + table_name

    schem = []
    data_df = dataframe.copy()

    data_df.columns=data_df.columns.str.replace('$','')
    data_df.columns=data_df.columns.str.replace('properties.','')
    
    dataset = bigquery_client.dataset('mixpanel')
    table_ref = dataset.table('events')
    
    data_df.fillna('0', inplace=True)
    
    # specific events that you want to get
    data_df = data_df[[ 'city', 'display', 'event', 'format',  
    'name', 'section', 'source', 'time', 'type', 'distinct_id']]
    
    
    for col in events_columns:
        if col not in data_df:
            data_df[col] = '0'
        if col == 'display':
            data_df[col] = data_df[col].astype(float)
        elif col == 'time':
            data_df[col] = data_df[col].astype(int)
        else:
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
    
# events which you want to get from mixpanel
events_to_use = [
    "Disable ads payed", "Disable ads pressed",
    "Initial data entered ", "Initial data opened", "Is it safe 75% opened",
    "Language selected", "Add shown"
]

events_columns = [ 'city', 'display', 'event', 'format',  
    'name', 'section', 'source', 'time', 'type', 'distinct_id']

    
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 8, 20),
    'provide_context': True,
    'min_file_process_interval': 1000,
    'retries': 0
       }

# run task every day
schedule = '@daily'
    
           
dag = DAG('upload_events', schedule_interval=schedule, default_args=args, catchup=False)

# date of last upload
last_upload = Variable.get("last_update_events")
last_date = datetime.datetime.strptime(Variable.get("last_update_events"),'%Y-%m-%dT00:00:00')
last_date = last_date + timedelta(days=1)
last_date = last_date.strftime('%Y-%m-%dT00:00:00')


        
def run_tasks(**kwargs):
    
    # select users from BQ table for which the events are not loaded yet
    query = '''
            select distinct_id 
            from mixpanel.users
            where events_loaded = False        
            '''

    credentials = service_account.Credentials.from_service_account_file(GCLOUD_JSON_KEY)

    df = pd.read_gbq(query, project_id=GCLOUD_PROJECT_ID, credentials=credentials)
    distinct_ids_max = 100
    dids = df['distinct_id'].to_list()

    # split distinct_ids list into sublists for using it in threads
    chunks_dids = [dids[x:x+distinct_ids_max] for x in range(0, len(dids), distinct_ids_max)]

    logging.warning(len(chunks_dids))
    
    for job_number in range(len(chunks_dids)):

        upload_events_to_bq = PythonOperator(task_id='upload_events_bq_{}'.format(str(job_number)),
                             python_callable=get_events,
                             provide_context=True,
                             op_kwargs={"start": last_upload,"end": last_date, 
                                        "num": chunks_dids[job_number]}, dag=dag)

        change_events = PythonOperator(task_id='change_flag_{}'.format(str(job_number)),
                         python_callable=change_events_fl,
                         provide_context=True,
                         op_kwargs={"num": chunks_dids[job_number]}, dag=dag)
        
        upload_events_to_bq.execute(kwargs)
        change_events.execute(kwargs)
        
        upload_events_to_bq >> change_events
        
    Variable.set("last_update_events", last_date)

        

run_dag = PythonOperator(task_id='run_tasks_upload',
                         python_callable=run_tasks,provides_context=True, dag=dag)