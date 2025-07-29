from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import os

def arquivo_existe():
    return os.path.exists("/tmp/arquivo_trigger.txt")

def processar_arquivo():
    print("Processando arquivo...")

with DAG(
    dag_id="dag_07_sensor",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sensor", "exercicio"]
) as dag:

    espera = PythonSensor(
        task_id="espera_arquivo",
        python_callable=arquivo_existe,
        mode="poke",
        poke_interval=10,
        timeout=300,
    )

    processa = PythonOperator(
        task_id="processar",
        python_callable=processar_arquivo
    )

    espera >> processa
