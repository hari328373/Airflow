
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
  
default_args={
    "owner":"airflow",
    "retries":5,
    "retry_delay":timedelta(minutes=2)
}
with DAG(
    dag_id='first_dag',
    schedule_interval="@daily",
    start_date=datetime(2022,12,21),
    catchup=False
    )as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo 'Hi from bash operator'",  
    )
    task1
    








