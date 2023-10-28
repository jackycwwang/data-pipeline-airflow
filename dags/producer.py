from airflow import DAG, Dataset
from airflow.decorators import task  # allows you to create PythonOperators faster

from datetime import datetime


my_file = Dataset(uri="/tmp/my_file.txt")   # my_file.txt will be automatically created

with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update")

    update_dataset()

    # What does outlets parameter mean: The outlets indicate that this task updates the dataset my_file. And it means as soon as the task update_dataset succeeds, the DAG that depends on this dataset (my_file) will automatically be triggered.





