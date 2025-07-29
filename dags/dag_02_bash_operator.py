from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_02_bash_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bash", "exercicio"]
) as dag:

    hello = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from BashOperator!'"
    )
