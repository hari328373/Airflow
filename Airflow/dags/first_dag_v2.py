
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
  
default_args={
    "owner":"airflow",
    "retries":5,
    "retry_delay":timedelta(minutes=2)
}
with DAG(
    dag_id='first_dag_v2',
    schedule_interval="@daily",
    start_date=datetime(2022,12,23),
    catchup=False
    )as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is first task",  
    )
    
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = 'echo this is second task , running after first task'
    )
    #task1.set_downstream(task2)
    task1 >> task2







