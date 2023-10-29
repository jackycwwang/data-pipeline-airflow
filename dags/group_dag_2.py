from airflow import DAG
from airflow.operators.bash import BashOperator

# * Since SubDagOperator is complicated to use, so it is deprecated. Instead, we are using TaskGroup.
# * With much less code, it is no need to use SubDagGroup to group our tasks.

# ^ * 0. Import these functions
from groups.group_downloads import download_tasks
from groups.group_transforms import transform_tasks


# #^ *  1. Remove SubDagOperator
# from airflow.operators.subdag import SubDagOperator

from datetime import datetime


with DAG('group_dag_TaskGroup', start_date=datetime(2023, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    # ^ * 2. Remove all args and SubDagOperators
    # args = {'start_date': dag.start_date,
    #         'schedule_interval': dag.schedule_interval,
    #         'catchup': dag.catchup
    #         }
    # downloads = SubDagOperator(
    #     task_id="downloads",
    #     # Note: The second argument below must be the same to the value of task_id above
    #     subdag=subdag_downloads(dag.dag_id, 'downloads', args=args)
    # )
    # transforms = SubDagOperator(
    #     task_id="transforms",
    #     subdag=subdag_transforms(dag.dag_id, 'transforms', args)
    # )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    # ^ * 3. Create a sub directory `groups` under `dags`, copy subdag_downloads.py and subdag_transforms.py from `subdags` into it. Rename these files to group_downloads.py and group_transforms.py respectively. Modify the contents of the files accordingly.

    # ^ * 4. call these functions

    downloads = download_tasks()
    transforms = transform_tasks()

    downloads >> check_files >> transforms
