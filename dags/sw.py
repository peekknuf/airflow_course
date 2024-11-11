from airflow.decorators import dag, task
from datetime import datetime


@dag(
    "xcom_practice",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def xcom_practice():
    @task
    def _transform():
        import requests

        resp = requests.get(f"https://swapi.dev/api/people/1/").json()
        print(resp)
        my_char = {}
        my_char["name"] = resp["name"]
        my_char["height"] = resp["height"]
        my_char["mass"] = resp["mass"]

        return my_char

    @task
    def _load(my_char):
        print(my_char)

    _load(_transform())


xcom_practice()
