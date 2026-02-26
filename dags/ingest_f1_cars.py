import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.setup_database import insert_from_dicts, get_latest_session_id, get_connection

# DAG Default Arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_cars_from_api():
    session_key = get_latest_session_id()
    if not session_key: return

    # 1. Get Session Start/End times to create time windows
    conn = get_connection()
    cur = conn.cursor()
    
    # We fetch the session times from our drivers table or session metadata
    sess_url = f"https://api.openf1.org/v1/sessions?session_key={session_key}"
    sess_info = requests.get(sess_url).json()[0]
    
    start_time = datetime.fromisoformat(sess_info['date_start'].replace('Z', '+00:00'))
    end_time = datetime.fromisoformat(sess_info['date_end'].replace('Z', '+00:00'))

    print(f"🧹 Clearing session {session_key}...")
    cur.execute("DELETE FROM raw.cars WHERE session_key = %s", (session_key,))
    
    # Get drivers from local DB
    cur.execute("SELECT DISTINCT driver_number FROM raw.drivers WHERE session_key = %s", (session_key,))
    driver_numbers = [row[0] for row in cur.fetchall()]
    conn.commit()
    cur.close()
    conn.close()

    # 2. Loop by Driver AND by Time Window (e.g., 20 minute chunks)
    interval = timedelta(minutes=20)
    
    for driver_num in driver_numbers:
        print(f"🏎️ Processing Driver {driver_num}...")
        current_start = start_time
        
        while current_start < end_time:
            current_end = current_start + interval
            
            params = {
                "session_key": session_key,
                "driver_number": driver_num,
                "date>": current_start.isoformat(),
                "date<": current_end.isoformat()
            }
            
            try:
                response = requests.get("https://api.openf1.org/v1/car_data", params=params, timeout=15)
                
                response.raise_for_status()
                car_data = response.json()

                if car_data:
                    for entry in car_data:
                        entry['recorded_at'] = entry.pop('date')
                    insert_from_dicts("raw.cars", car_data)
                
                current_start = current_end
                time.sleep(0.5)

            except Exception as e:
                print(f"⚠️ Error in window {current_start} for driver {driver_num}: {e}")
                current_start = current_end
                continue

    print(f"🏁 Telemetry complete for session {session_key}")

with DAG(
    'f1_ingest_cars_staging',
    default_args=default_args,
    description='Full pipeline: API -> Postgres -> Parquet',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'automated'],
) as dag:

    # Step 1: Ingest from API to Postgres
    task_ingest_api = PythonOperator(
        task_id='ingest_cars_api_to_postgres',
        python_callable=ingest_cars_from_api,
    )

    # Step 2: Export from Postgres to Parquet
    task_export_parquet = BashOperator(
        task_id='export_postgres_to_parquet',
        bash_command='python3 /opt/airflow/scripts/export_to_parquet.py',
    )

    task_ingest_api >> task_export_parquet