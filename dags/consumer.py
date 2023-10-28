from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime


my_file = Dataset(uri="/tmp/my_file.txt")   # Need to use the same dataset as does the producer

with DAG(
    dag_id="consumer",
    schedule=[my_file],     # my_file update (from producer) will trigger the consumer DAG
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    @task       # No need for outlets parameter because this task doesn't update the dataset
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    read_dataset()