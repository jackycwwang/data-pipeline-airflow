from airflow import DAG
from airflow.operators.bash import BashOperator


def subdag_downloads(parent_dag_id, child_dag_id, args):
    # You have to make sure some args are the same between sub dags and the parent dags, such as schedule interval, the start_date, or catchup parameters. Otherwise you will end up with weird behavior.
    with DAG(f"{parent_dag_id}.{child_dag_id}",
             start_date=args['start_date'],
             schedule_interval=args['schedule_interval'],
             catchup=args['catchup']
             ) as dag:

        download_a = BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )

        download_b = BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )

        download_c = BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )

    return dag
