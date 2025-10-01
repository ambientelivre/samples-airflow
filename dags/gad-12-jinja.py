from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='jinja_example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Este operador vai criar um arquivo com a data do dia usando Jinja
    create_file = BashOperator(
        task_id='create_file',
        bash_command='echo "Data de hoje é {{ ds }}" > /tmp/data_{{ ds_nodash }}.txt'
    )

    # Apenas para mostrar na tela o arquivo criado
    show_file = BashOperator(
        task_id='show_file',
        bash_command='cat /tmp/data_{{ ds_nodash }}.txt'
    )

    create_file >> show_file
