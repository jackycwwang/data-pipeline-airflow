from airflow import DAG

# ^ * Import branch operator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


# * To share data between t1 and t2, first we need to push the data to XCom.
# * There are two ways to do this:
# * 1. By returning the value (from Python callable function). The key associated with the value is "return_value"
# * 2. If you'd like to define your own key for the value, you can use xcom_push() method of the task instance (short for "ti" in convention).


def _t1(ti):
    ti.xcom_push(key="my_key", value=42)


def _t2(ti):
    print(ti.xcom_pull(key="my_key", task_ids="t1"))


# ^ * Define the branch function
def _branch(ti):
    value = ti.xcom_pull(key="my_key", task_ids="t1")
    if value == 42:
        return 't2'
    return 't3'


with DAG("xcom_dag", start_date=datetime(2021, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    # ^ * Create a new task as the branch task
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=_branch
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )

    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''",
        trigger_rule='none_failed_min_one_success'  #^ Define trigger rule for this downstream task
    )

    # ^ * Define dependencies
    # t1 >> t2 >> t3
    t1 >> branch >> [t2, t3] >> t4
