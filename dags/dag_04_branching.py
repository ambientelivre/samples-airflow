from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import random

def escolher_caminho():
    return "caminho_a" if random.choice([True, False]) else "caminho_b"

with DAG(
    dag_id="dag_04_branching",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["branch", "exercicio"]
) as dag:

    start = BranchPythonOperator(
        task_id="escolher_caminho",
        python_callable=escolher_caminho
    )

    caminho_a = DummyOperator(task_id="caminho_a")
    caminho_b = DummyOperator(task_id="caminho_b")

    start >> [caminho_a, caminho_b]
