import logging
from datetime import timedelta, datetime
import json
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import AirflowException
from google.oauth2 import service_account

import requests
import pandas as pd
import datetime
import urllib.request, urllib.parse, urllib.error
import time
import pytz
import io
import os

import requests
from mixpanel_api import Mixpanel
from pandas.io.json import json_normalize 
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from dateutil.relativedelta import relativedelta

from io import StringIO
from appsflyer import AppsFlyer


pd.set_option('display.max_columns', None)

VERSION = '2.0'  # Mixpanel API version
date_format = '%Y-%m-%d'  
APPSFLYER_API_TOKEN = ''
APPSFLYER_API_ID = ''

GCLOUD_JSON_KEY ='key.json' # Gcloud's JSON KEY
GCLOUD_PROJECT_ID = '' # Gcloud's PROJECT KEY
DATASET_BIGQUERY = 'appmetrica' # BigQuery's dataset name

bigquery_client = bigquery.Client.from_service_account_json(os.path.relpath(GCLOUD_JSON_KEY))
dataset_ref = bigquery_client.dataset(DATASET_BIGQUERY)


events_appsflyer_columns = [
    'Attributed_Touch_Type', 'Attributed_Touch_Time', 'Install_Time',
    'Event_Time', 'Event_Name', 'Event_Value', 'Event_Revenue',
    'Event_Revenue_Currency', 'Event_Revenue_Preferred', 'Event_Source',
    'Is_Receipt_Validated', 'Partner', 'Media_Source', 'Channel', 'Keywords',
    'Campaign', 'Campaign_ID', 'Adset', 'Adset_ID', 'Ad', 'Ad_ID', 'Ad_Type',
    'Site_ID', 'Sub_Site_ID', 'Sub_Param_1', 'Sub_Param_2', 'Sub_Param_3',
    'Sub_Param_4', 'Sub_Param_5', 'Cost_Model', 'Cost_Value', 'Cost_Currency',
    'Contributor_1_Partner', 'Contributor_1_Media_Source',
    'Contributor_1_Campaign', 'Contributor_1_Touch_Type',
    'Contributor_1_Touch_Time', 'Contributor_2_Partner',
    'Contributor_2_Media_Source', 'Contributor_2_Campaign',
    'Contributor_2_Touch_Type', 'Contributor_2_Touch_Time',
    'Contributor_3_Partner', 'Contributor_3_Media_Source',
    'Contributor_3_Campaign', 'Contributor_3_Touch_Type',
    'Contributor_3_Touch_Time', 'Region', 'Country_Code', 'State', 'City',
    'Postal_Code', 'DMA', 'IP', 'WIFI', 'Operator', 'Carrier', 'Language',
    'AppsFlyer_ID', 'Advertising_ID', 'IDFA', 'Android_ID', 'Customer_User_ID',
    'IMEI', 'IDFV', 'Platform', 'Device_Type', 'OS_Version', 'App_Version',
    'SDK_Version', 'App_ID', 'App_Name', 'Bundle_ID', 'Is_Retargeting',
    'Retargeting_Conversion_Type', 'Attribution_Lookback',
    'Reengagement_Window', 'Is_Primary_Attribution', 'User_Agent',
    'HTTP_Referrer', 'Original_URL', 'Install_App_Store', 'Match_Type',
    'Contributor_1_Match_Type', 'Contributor_2_Match_Type',
    'Contributor_3_Match_Type', 'Device_Category', 'Google_Play_Referrer',
    'Google_Play_Click_Time', 'Google_Play_Install_Begin_Time'
]

def if_tbl_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except:
        return False

def upload_to_bq(table_name, dataframe):

    table_id = GCLOUD_PROJECT_ID + '.' + DATASET_BIGQUERY + '.' + table_name
    table_ref = dataset_ref.table(table_name)

    schem = []
    data_df = dataframe.copy()
    
    data_df.rename(columns={
    'Attributed Touch Type': 'Attributed_Touch_Type',
    'Attributed Touch Time': 'Attributed_Touch_Time',
    'Install Time': 'Install_Time',
    'Event Time': 'Event_Time',
    'Event Name': 'Event_Name',
    'Event Value': 'Event_Value',
    'Event Revenue': 'Event_Revenue',
    'Event Revenue Currency': 'Event_Revenue_Currency',
    'Event Revenue Preferred': 'Event_Revenue_Preferred',
    'Event Source': 'Event_Source',
    'Is Receipt Validated': 'Is_Receipt_Validated',
    'Media Source': 'Media_Source',
    'Campaign ID': 'Campaign_ID',
    'Adset ID': 'Adset_ID',
    'Ad ID': 'Ad_ID',
    'Ad Type': 'Ad_Type',
    'Site ID': 'Site_ID',
    'Sub Site ID': 'Sub_Site_ID',
    'Sub Param 1': 'Sub_Param_1',
    'Sub Param 2': 'Sub_Param_2',
    'Sub Param 3': 'Sub_Param_3',
    'Sub Param 4': 'Sub_Param_4',
    'Sub Param 5': 'Sub_Param_5',
    'Cost Model': 'Cost_Model',
    'Cost Value': 'Cost_Value',
    'Cost Currency': 'Cost_Currency',
    'Contributor 1 Partner': 'Contributor_1_Partner',
    'Contributor 1 Media Source': 'Contributor_1_Media_Source',
    'Contributor 1 Campaign': 'Contributor_1_Campaign',
    'Contributor 1 Touch Type': 'Contributor_1_Touch_Type',
    'Contributor 1 Touch Time': 'Contributor_1_Touch_Time',
    'Contributor 2 Partner': 'Contributor_2_Partner',
    'Contributor 2 Media Source': 'Contributor_2_Media_Source',
    'Contributor 2 Campaign': 'Contributor_2_Campaign',
    'Contributor 2 Touch Type': 'Contributor_2_Touch_Type',
    'Contributor 2 Touch Time': 'Contributor_2_Touch_Time',
    'Contributor 3 Partner': 'Contributor_3_Partner',
    'Contributor 3 Media Source': 'Contributor_3_Media_Source',
    'Contributor 3 Campaign': 'Contributor_3_Campaign',
    'Contributor 3 Touch Type': 'Contributor_3_Touch_Type',
    'Contributor 3 Touch Time': 'Contributor_3_Touch_Time',
    'Country Code': 'Country_Code',
    'Postal Code': 'Postal_Code',
    'AppsFlyer ID': 'AppsFlyer_ID',
    'Advertising ID': 'Advertising_ID',
    'Android ID': 'Android_ID',
    'Customer User ID': 'Customer_User_ID',
    'Device Type': 'Device_Type',
    'OS Version': 'OS_Version',
    'App Version': 'App_Version',
    'SDK Version': 'SDK_Version',
    'App ID': 'App_ID',
    'App Name': 'App_Name',
    'Bundle ID': 'Bundle_ID',
    'Is Retargeting': 'Is_Retargeting',
    'Retargeting Conversion Type': 'Retargeting_Conversion_Type',
    'Attribution Lookback': 'Attribution_Lookback',
    'Reengagement Window': 'Reengagement_Window',
    'Is Primary Attribution': 'Is_Primary_Attribution',
    'User Agent': 'User_Agent',
    'HTTP Referrer': 'HTTP_Referrer',
    'Original URL': 'Original_URL',
    'Install App Store': 'Install_App_Store',
    'Match Type': 'Match_Type',
    'Contributor 1 Match Type': 'Contributor_1_Match_Type',
    'Contributor 2 Match Type': 'Contributor_2_Match_Type',
    'Contributor 3 Match Type': 'Contributor_3_Match_Type',
    'Device Category': 'Device_Category',
    'Google Play Referrer': 'Google_Play_Referrer',
    'Google Play Click Time': 'Google_Play_Click_Time',
    'Google Play Install Begin Time': 'Google_Play_Install_Begin_Time'}, inplace=True)
    
    
    data_df = data_df[['Attributed_Touch_Type', 'Attributed_Touch_Time', 'Install_Time',
    'Event_Time', 'Event_Name', 'Event_Value', 'Event_Revenue',
    'Event_Revenue_Currency', 'Event_Revenue_Preferred', 'Event_Source',
    'Is_Receipt_Validated', 'Partner', 'Media_Source', 'Channel', 'Keywords',
    'Campaign', 'Campaign_ID', 'Adset', 'Adset_ID', 'Ad', 'Ad_ID', 'Ad_Type',
    'Site_ID', 'Sub_Site_ID', 'Sub_Param_1', 'Sub_Param_2', 'Sub_Param_3',
    'Sub_Param_4', 'Sub_Param_5', 'Cost_Model', 'Cost_Value', 'Cost_Currency',
    'Contributor_1_Partner', 'Contributor_1_Media_Source',
    'Contributor_1_Campaign', 'Contributor_1_Touch_Type',
    'Contributor_1_Touch_Time', 'Contributor_2_Partner',
    'Contributor_2_Media_Source', 'Contributor_2_Campaign',
    'Contributor_2_Touch_Type', 'Contributor_2_Touch_Time',
    'Contributor_3_Partner', 'Contributor_3_Media_Source',
    'Contributor_3_Campaign', 'Contributor_3_Touch_Type',
    'Contributor_3_Touch_Time', 'Region', 'Country_Code', 'State', 'City',
    'Postal_Code', 'DMA', 'IP', 'WIFI', 'Operator', 'Carrier', 'Language',
    'AppsFlyer_ID', 'Advertising_ID', 'IDFA', 'Android_ID', 'Customer_User_ID',
    'IMEI', 'IDFV', 'Platform', 'Device_Type', 'OS_Version', 'App_Version',
    'SDK_Version', 'App_ID', 'App_Name', 'Bundle_ID', 'Is_Retargeting',
    'Retargeting_Conversion_Type', 'Attribution_Lookback',
    'Reengagement_Window', 'Is_Primary_Attribution', 'User_Agent',
    'HTTP_Referrer', 'Original_URL', 'Install_App_Store', 'Match_Type',
    'Contributor_1_Match_Type', 'Contributor_2_Match_Type',
    'Contributor_3_Match_Type', 'Device_Category', 'Google_Play_Referrer',
    'Google_Play_Click_Time', 'Google_Play_Install_Begin_Time']]


    for col in events_appsflyer_columns:         
        data_df[col] = data_df[col].astype(str) 

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
                
    logging.warning(data_df.head(2))           

    job_config = bigquery.LoadJobConfig(schema=schem)

    job = bigquery_client.load_table_from_dataframe(data_df, table_id, job_config=job_config)        

    
def to_df(resp):
    if resp.status_code != requests.codes.ok:
        raise Exception(resp.text)

    return pd.read_csv(StringIO(resp.text))



def get_events(*op_args, conf=None, **context):
    
    logging.warning('This is a context message')
    logging.warning(context)
    
    start_date = op_args[0]
    end_date = str(op_args[1])
    
    logging.warning(start_date)
    logging.warning(end_date)
    
    af = AppsFlyer(api_token=APPSFLYER_API_TOKEN,
               app_id=APPSFLYER_API_ID)
    
    
    in_app_events_report = af.in_app_events_report(date_from=start_date, date_to=end_date)
    in_app_events_report = to_df(in_app_events_report)
    
    logging.warning(in_app_events_report.head(2))

    if in_app_events_report.shape[0] == 200000:
        end_date = in_app_events_report['Event Time'].max()
    
    logging.warning(in_app_events_report.head(2))


    upload_to_bq('events_appsflyer', in_app_events_report)    
    
    now = datetime.datetime.strptime(time.strftime("%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00')  
    
    end_date_date = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

    
    if end_date_date <= now: 
        Variable.set("last_update_events_appsflyer", end_date)
    else:
        return None

    return str(conf)


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 7, 19),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}


hours_added = timedelta(hours = 10)
start_date =  datetime.datetime.strptime(Variable.get("last_update_events_appsflyer"),'%Y-%m-%d %H:%M:%S')
finish_date = start_date + hours_added



with DAG('events_appsflyer',
         default_args=args,
         schedule_interval='*/10 * * * *'
         ) as dag:
    
    
    get_events = PythonOperator(task_id='update_users',
                                        provide_context=True,
                                        python_callable=get_events,
                                        op_args=[start_date, finish_date])
    
