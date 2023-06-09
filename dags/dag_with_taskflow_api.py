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

@dag(dag_id='dag_with_taskflow_api_v2',
     default_args=default_args,
     start_date=datetime(2021,10,26),
     schedule_interval='@daily')
def hello_world_etl():
    
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'firstname':'Pooja',
            'lastname':'Patel'
            }
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(firstname,lastname,age):
        print(f"Hello world, my name is {firstname},{lastname} and i am {age} years old")
        
    name_dict= get_name()
    age=get_age()
    greet(firstname=name_dict['firstname'],lastname=name_dict['lastname'],age=age)
    
  
greet_dag = hello_world_etl()  