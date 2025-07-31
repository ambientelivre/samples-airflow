from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def extract_pets(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT species FROM pet;")
    dados = cursor.fetchall()

    # Exemplo: [('dog',), ('cat',), ('dog',), ...]
    species_list = [row[0] for row in dados]
    context['ti'].xcom_push(key='species_list', value=species_list)

def transform_aggregate(**context):
    from collections import Counter

    species_list = context['ti'].xcom_pull(key='species_list', task_ids='extract')
    contagem = dict(Counter(species_list))  # {'dog': 3, 'cat': 2}

    total = sum(contagem.values())
    print(f"Total de pets: {total}")
    print(f"Contagem por espécie: {contagem}")

    context['ti'].xcom_push(key='dados_aggregados', value=contagem)

def load_aggregated(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    dados = context['ti'].xcom_pull(key='dados_aggregados', task_ids='transform')

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pet_aggregado (
            species TEXT,
            total INT
        );
    """)
    cursor.execute("DELETE FROM pet_aggregado;")

    for especie, total in dados.items():
        cursor.execute(
            "INSERT INTO pet_aggregado (species, total) VALUES (%s, %s);",
            (especie, total)
        )

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='etl_pet_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "postgres", "xcom"]
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_pets,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_aggregate,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_aggregated,
        provide_context=True
    )

    extract >> transform >> load
