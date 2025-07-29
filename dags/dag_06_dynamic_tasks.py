from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_06_dynamic_tasks",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dinamico", "exercicio"]
) as dag:

    for i in range(5):
        BashOperator(
            task_id=f"print_{i}",
            bash_command=f"echo 'Tarefa número {i}'"
        )
