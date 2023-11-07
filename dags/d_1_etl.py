from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Connection

from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import requests
import pandas as pd
import re


# MODULE
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

def transform_extract_user_agent(items):
    user_agent = items['UserAgent']
    browser_regex = r"(\w+)/\d+\.\d+"
    os_regex = r"\(([^;]+)"
    # Extract browser and OS information
    browser_match = re.search(browser_regex, user_agent)
    os_match = re.search(os_regex, user_agent)

    browser_info = browser_match.group(1) if browser_match else "N/A"
    os_info = os_match.group(1) if os_match else "N/A"

    new_columns = pd.Series({
        'browser_info': browser_info,
        'operating_system': os_info
    })
    return new_columns
# END MODULE


#function check
def fun_check_if_file_exists(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print('check date: ', execution_date)
    for file in get_file_list('1Uv3Y4nlkIElxcjUlCS6ujsuNhGuEAMNM'):
        if execution_date in file['name']:
            return True
    raise AirflowSkipException

#function etl
def fun_etl(**context):
    # extract
    data = None
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    print('check date: ', execution_date)
    for file in get_file_list('1Uv3Y4nlkIElxcjUlCS6ujsuNhGuEAMNM'):
        if execution_date in file['name']:
            download_from_fileid(file['id'], file['name'])
            data = pd.read_csv(file['name'])
            break

    try:
        # transform
        print("Data from Google Drive : ")
        print(data.head())
        print(data.info())
        new_columns = data.apply(transform_extract_user_agent, axis=1)
        df = pd.concat([data, new_columns], axis=1)
        df['execution_date'] = execution_date
        print("After Transform : ")
        print(df.head())
        print(df.info())
    except Exception as e:
        print('Invalid file.!')
        print(e)

    # load
    config = Connection.get_connection_from_secrets("my-postgres")
    db_params = {
        "user": config.login,
        "password": config.password,
        "host": config.host,
        "port": config.port,
        "database": config.schema
    }
    engine = create_engine(
        f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
    schema_name = "public"
    table_name = "users_visitor_denise"
    df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
    engine.dispose()

# THE DAG
with DAG(
    dag_id='d_1_etl',
    start_date=datetime(2023, 10, 1),
    schedule_interval='0 23 * * *',

    #catch up true berarti akan running mengikuti start date nya.
    #catch up false berarti running nya interval terakhir
    catchup=True,

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
