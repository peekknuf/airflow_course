from airflow.decorators import dag, task 
from datetime import datetime, timedelta
from time import sleep
from airflow.decorators.bash import BashOperator
from airflow.decorators.python import PythonOperator

default_args = {
    "owner": "maksym ionutsa",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)}

@dag (
    default_args=default_args,
    dag_id="checking_file",
    description="checking dag",
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["practice", "testing"],
    catchup=False)

def my_dag():

    @task
    def create_file():
        task_id='we creating file'
        bash_command='echo "hello world" > /tmp/test.txt'
    @task
    def check_file():
        task_id='we checking file'
        bash_command='test -f /tmp/test.txt'
    @task
    def read_file():
        task_id='we reading file'
        python_callable=lambda:open('/tmp/test.txt').read()
    @task
    def delete_file():
        task_id='we deleting file'
        bash_command='rm /tmp/test.txt'


    create_file() >> check_file() >> read_file() >> delete_file() 

my_dag()
