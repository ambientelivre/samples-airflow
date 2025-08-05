from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="echo_hostname_dag",
    start_date=datetime(2024, 1, 22),
    schedule_interval=None,
    catchup=False,
) as dag:

    echo_hostname_task = BashOperator(
        task_id="echo_hostname",
        queue="queue_q1",
        bash_command="echo $(hostname)",
    )

echo_hostname_task
