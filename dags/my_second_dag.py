from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'pooja',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

def greet(age,ti):
    firstname=ti.Xcom_pull(task_ids='getName',key='first_name')
    lastname = ti.Xcom_pull(task_ids='getName',key='last_name')
    print(f"Hello! my name is {firstname},{lastname} and i am {age} years old")
    
def getName(ti):
    ti.Xcom_push(key='first_name',value='Pooja')
    ti.Xcom_push(key='last_name',value='Patel')

with DAG(
    dag_id = 'our_second_dag_v5',
    default_args = default_args,
    description= ' This is our second dag that we write',
    start_date=datetime(2021,10,6),
    schedule_interval='@daily'
    ) as dag:
    task1 = PythonOperator(
        task_id = 'first_task',
        python_callable = greet,
        op_kwargs={'age':24}
    )
    
    task2 = PythonOperator(
        task_id = 'second-task',
        python_callable=getName
    )
    
    task2 >> task1