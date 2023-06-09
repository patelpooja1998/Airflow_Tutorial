from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'pooja',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = 'our_first_dag',
    default_args = default_args,
    description= ' This is our first dag that we write',
    start_date=datetime(2021,7,29,2),
    schedule_interval='@daily'
    ) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command= "echo hello world, This is the first task"
    )
    
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command="echo I am the second task, i am going to run after task 1"
    )
    
    task1.set_downstream(task2)