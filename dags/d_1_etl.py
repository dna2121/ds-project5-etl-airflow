from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

#function check
def fun_check_if_file_exists(**context):
    pass

#function etl
def fun_etl(**context):
    pass

with DAG(
    dag_id='d_1_etl',
    start_date=datetime(2023, 10, 1),
    schedule_interval='0 23 * * *',

    #catch up true berarti akan running mengikuti start date nya.
    #catch up false berarti running nya interval terakhir
    catchup=True

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
