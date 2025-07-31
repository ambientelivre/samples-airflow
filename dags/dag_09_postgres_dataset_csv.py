from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv
import os
from collections import Counter

# Caminho para salvar o CSV
DATASET_PATH = os.path.join(os.path.dirname(__file__), 'datasets', 'pets.csv')

def extract_to_csv():
    hook = PostgresHook(postgres_conn_id='my_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT name, species FROM pet;")
    rows = cursor.fetchall()

    os.makedirs(os.path.dirname(DATASET_PATH), exist_ok=True)

    with open(DATASET_PATH, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'species'])  # Cabeçalho
        writer.writerows(rows)

    cursor.close()
    conn.close()

def transform_and_load():
    hook = PostgresHook(postgres_conn_id='my_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open(DATASET_PATH, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        species_list = [row['species'] for row in reader]

    contagem = Counter(species_list)  # {'dog': 3, 'cat': 2}

    # Cria tabela se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pet_aggregado (
            species TEXT,
            total INT
        );
    """)
    cursor.execute("DELETE FROM pet_aggregado;")

    for especie, total in contagem.items():
        cursor.execute(
            "INSERT INTO pet_aggregado (species, total) VALUES (%s, %s);",
            (especie, total)
        )

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='etl_pet_csv_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'csv', 'datasets']
) as dag:

    extract = PythonOperator(
        task_id='extract_to_csv',
        python_callable=extract_to_csv
    )

    transform_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load
    )

    extract >> transform_load
