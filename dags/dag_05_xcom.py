from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_numero(**context):
    context['ti'].xcom_push(key='numero', value=42)

def imprimir_numero(**context):
    numero = context['ti'].xcom_pull(task_ids='gerar', key='numero')
    print(f"Número recebido via XCom: {numero}")

with DAG(
    dag_id="dag_05_xcom",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["xcom", "exercicio"]
) as dag:

    gerar = PythonOperator(
        task_id="gerar",
        python_callable=gerar_numero,
        provide_context=True,
    )

    imprimir = PythonOperator(
        task_id="imprimir",
        python_callable=imprimir_numero,
        provide_context=True,
    )

    gerar >> imprimir
