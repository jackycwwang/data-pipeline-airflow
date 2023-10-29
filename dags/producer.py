from airflow import DAG, Dataset
from airflow.decorators import task  # allows you to create PythonOperators faster

from datetime import datetime


my_file = Dataset(uri="/tmp/my_file.txt")   # my_file.txt will be automatically created
my_file_2 = Dataset(uri="/tmp/my_file_2.txt")

with DAG(
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
):

    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update my_file.txt")

    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            f.write("producer update my_file_2.txt")

    update_dataset()
    update_dataset_2()

    # What does outlets parameter mean: The outlets indicate that this task updates the dataset my_file. And it means as soon as the task update_dataset succeeds, the DAG that depends on this dataset (my_file) will automatically be triggered.





