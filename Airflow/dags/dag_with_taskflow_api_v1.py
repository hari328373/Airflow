
# it automatically defines the task dependencies

from airflow.decorators import dag,task 
from datetime import datetime,timedelta

default_args = {
    'owner':'airflow',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

@dag(dag_id='dag_with_taskflow_api_v1',
     default_args=default_args,
     start_date=datetime(2022,12,27),
     schedule_interval='@daily')

def hello_world_etl():
    
    
    @task()
    def get_name():
        return "jerry"
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(name,age):
        print(f"Hello world! My name is {name} "
              f"and i am {age} years old!")

    name = get_name()
    age=get_age()

    greet(name=name, age=age)

# instance of DAG
greet_dag = hello_world_etl()
