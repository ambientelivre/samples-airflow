from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="dag_01_hello_world",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["exercicio", "inicio"],
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    start >> end
