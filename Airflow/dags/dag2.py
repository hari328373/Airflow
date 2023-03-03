
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
  
default_args={
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(minutes=2)
}

def greet(age,ti):
    name=ti.xcom_pull(task_ids='get_name')      # every return value push into x_coms --> that pushed is pulled use in other task
    p=print(f"Hello World! My name is {name},"
            f"and my age is {age}")
    return p  

def get_name():
    return 'jerry'
    
with DAG(
    dag_id='dag2',
    schedule_interval="@daily",
    start_date=datetime(2022,12,27),
    catchup=False
    )as dag:
    
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age':20}
    )
    
    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name    
    )
    task2 >> task1  # here task2 is push and after task1 is pull that value so, task2 is up_stream of task1








