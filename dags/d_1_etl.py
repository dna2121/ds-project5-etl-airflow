from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import requests
import pandas as pd
import re


#module
def get_file_list(folder_id):
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/dags/credentials/service-account.json', scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    drive_service = build('drive', 'v3', credentials=credentials)
    
    results = drive_service.files().list(
        q=f"'{folder_id}' in parents", fields="files(id, name)"
    ).execute()

    files = results.get('files', [])

    if not files:
        print('No files found in the folder.')
        return []
    else:
        data = []
        print('Files in the folder:')
        for file in files:
            print(f'{file["name"]} ({file["id"]})')
            data.append({'name': file["name"], 'id':file["id"]})
        return data

def download_from_fileid(file_id, filename):
    csv_url = f'https://drive.google.com/uc?id={file_id}'
    print(f'downlading ... {csv_url}')
    response = requests.get(csv_url)
    if response.status_code == 200:
        with open(f'{filename}', 'wb') as file:
            file.write(response.content)
    else:
        print("Failed to download the CSV.")
#end

#function check
def fun_check_if_file_exists(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print('check date: ', execution_date)
    for file in get_file_list('181R-ug68jYWaI9vBttHnHbt7cTQbuZ1s'):
        if execution_date in file['name']:
            return True
    raise AirflowSkipException

#function etl
def fun_etl(**context):
    pass

with DAG(
    dag_id='d_1_etl',
    start_date=datetime(2023, 10, 1),
    schedule_interval='0 23 * * *',

    #catch up true berarti akan running mengikuti start date nya.
    #catch up false berarti running nya interval terakhir
    catchup=False,

    #supaya run nya maksimal 1 atau brp yg ditentukan
    max_active_runs=1 
) as dag:

    #operator start
    op_start = EmptyOperator(
        task_id='start'
    )

    #check
    op_check_file = PythonOperator(
        task_id='check_if_file_exists',
        python_callable=fun_check_if_file_exists
    )

    #etl
    op_etl = PythonOperator(
        task_id='etl',
        python_callable=fun_etl
    )

    #end
    op_end = EmptyOperator(
        task_id='end'
    )

    #flow in dag
    op_start >> op_check_file >> op_etl >> op_end
