from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag,task

default_args = {
    'owner' : 'pooja',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id='dag_With_catchup_and_backfill_v2',
    default_args=default_args,
    start_date=datetime(2021,11,1),
    schedule_interval='@daily',
    catchup=False
    ) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo this is a bash command",
        
    )