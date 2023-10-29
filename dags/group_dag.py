from airflow import DAG
from airflow.operators.bash import BashOperator

#^ *  1. Import subdag subdag_downloads
from subdags.subdag_downloads import subdag_downloads
from subdags.subdag_transforms import subdag_transforms

#^ *  2. Import SubDagOperator
from airflow.operators.subdag import SubDagOperator

from datetime import datetime


with DAG('group_dag', start_date=datetime(2023, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    #^ * 3. Create args of the parent dag
    args = {'start_date': dag.start_date,
            'schedule_interval': dag.schedule_interval,
            'catchup': dag.catchup
            }

    #^ * 4 Remove the tasks we have moved to the subdag
    # download_a = BashOperator(
    #     task_id='download_a',
    #     bash_command='sleep 10'
    # )

    # download_b = BashOperator(
    #     task_id='download_b',
    #     bash_command='sleep 10'
    # )

    # download_c = BashOperator(
    #     task_id='download_c',
    #     bash_command='sleep 10'
    # )
    #^ * 5. Replace them with the SubDagOperator
    downloads = SubDagOperator(
        task_id="downloads",
        # Note: The second argument below must be the same to the value of task_id above
        subdag=subdag_downloads(dag.dag_id, 'downloads', args=args)
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    # transform_a = BashOperator(
    #     task_id='transform_a',
    #     bash_command='sleep 10'
    # )

    # transform_b = BashOperator(
    #     task_id='transform_b',
    #     bash_command='sleep 10'
    # )

    # transform_c = BashOperator(
    #     task_id='transform_c',
    #     bash_command='sleep 10'
    # )
    transforms = SubDagOperator(
        task_id="transforms",
        subdag=subdag_transforms(dag.dag_id, 'transforms', args)
    )

    #^ * 6. Change the dependency graph
    # [download_a, download_b, download_c] >> check_files >> [
    #     transform_a, transform_b, transform_c]

    # downloads >> check_files >> [
    #     transform_a, transform_b, transform_c]
    downloads >> check_files >> transforms
