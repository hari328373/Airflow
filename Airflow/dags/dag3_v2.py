
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
  
default_args={
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(minutes=2)
}

def greet(ti):
    first_name=ti.xcom_pull(task_ids='get_name',key='first_name')
    last_name=ti.xcom_pull(task_ids='get_name',key='last_name') 
    age=ti.xcom_pull(task_ids='get_age',key='age')
    p=print(f"Hello World! My name is {first_name} {last_name},"
            f"and my age is {age}")
    return p  

def get_name(ti):
    ti.xcom_push(key='first_name',value='jerry')
    ti.xcom_push(key='last_name',value='Gideon')
    
def get_age(ti):
    ti.xcom_push(key='age',value=19)
    
with DAG(
    dag_id='dag3_v2',
    schedule_interval="@daily",
    start_date=datetime(2022,12,27),
    catchup=False
    )as dag:
    
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'age':20}
    )
    
    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name    
    )
    
    task3=PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    
    [task2,task3] >> task1  








