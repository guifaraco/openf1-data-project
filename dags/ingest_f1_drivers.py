from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.setup_database import insert_from_dicts
from scripts.utils import get_active_session_key
import requests

# DAG Default Arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_drivers_from_api():
    """
    Fetches drivers for the latest session and performs an atomic Overwrite.
    """
    # Get the same anchor session
    session_key = get_active_session_key()
    if not session_key:
        return

    # Fetch filtered data
    url = "https://api.openf1.org/v1/drivers"
    params = {"session_key": session_key}
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    drivers = response.json()

    if not drivers:
        print(f"No drivers found for session {session_key}.")
        return
    
    for driver in drivers:
        driver.pop('country_code', None)

    insert_from_dicts(
        table_name="raw.drivers", 
        data_dicts=drivers, 
        batch_size=1000, 
        delete_key=("session_key", session_key)
    )
    print(f"✅ Ingested {len(drivers)} drivers for session {session_key}.")

with DAG(
    'f1_ingest_drivers_staging',
    default_args=default_args,
    description='Full pipeline: API -> Postgres -> Parquet',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'automated'],
) as dag:

    # Ingest from API to Postgres
    task_ingest_api = PythonOperator(
        task_id='ingest_drivers_api_to_postgres',
        python_callable=ingest_drivers_from_api,
    )

    # Export from Postgres to Parquet
    task_export_parquet = BashOperator(
        task_id='export_postgres_to_parquet',
        bash_command='python3 /opt/airflow/scripts/export_to_parquet.py',
    )

    task_ingest_api >> task_export_parquet