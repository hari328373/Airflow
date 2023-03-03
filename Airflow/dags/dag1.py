
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
  
default_args={
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(minutes=2)
}
  
def greet(name,age):
    p=print(f"Hello World! My name is {name},"
            f"and my age is {age}")
    return p  
    
with DAG(
    dag_id='dag1',
    schedule_interval="@daily",
    start_date=datetime(2022,12,27),
    catchup=False
    )as dag:
    
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name':'John','age':20}
        )
    task1
    








