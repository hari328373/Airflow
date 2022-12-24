
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
  
default_args={
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(minutes=2)
}
  
def function1_execute():
    p=print("Hello World")
    return p  
    
with DAG(
    dag_id='second_dag',
    schedule_interval="@daily",
    start_date=datetime(2022,12,23,12),
    catchup=False
    )as dag:
    
    function1_execute=PythonOperator(
        task_id='function1_execute',
        python_callable=function1_execute)
    
    








