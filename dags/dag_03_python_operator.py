from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Olá, mundo!")

with DAG(
    dag_id="dag_03_python_operator",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["python", "exercicio"]
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )
