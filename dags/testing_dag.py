from airflow.decorators import dag, task 
from datetime import datetime, timedelta
from time import sleep

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
    dag_id="testing_dag",
    description="Testing DAG",
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["practice", "testing"],
    catchup=False)

def my_dag():

    @task
    def print_hello():
        print("hello world")

    @task
    def wait():
        sleep(3)

    @task
    def wait1():
        sleep(5)

    @task
    def wait2():
        sleep(7)

    @task
    def print_goodbye():
        print("goodbye world")

   
    print_hello() >> [wait(), wait1(), wait2()] >> print_goodbye()

my_dag()

