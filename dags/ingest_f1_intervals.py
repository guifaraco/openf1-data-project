import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.setup_database import insert_from_dicts, get_connection
from scripts.utils import get_active_session_key, get_start_and_end_time

# DAG Default Arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_intervals_from_api():
    session_key = get_active_session_key()
    if not session_key:
        print("❌ No session_key found.")
        return

    start_time, end_time = get_start_and_end_time(session_key)
    if not start_time or not end_time:
        print(f"❌ Could not determine boundaries for session {session_key}")
        return

    # Database Cleanup
    conn = get_connection()
    cur = conn.cursor()
    print(f"🧹 Clearing existing intervals for session {session_key}...")
    cur.execute("DELETE FROM raw.intervals WHERE session_key = %s", (session_key,))
    conn.commit()
    cur.close()
    conn.close()

    interval_window = timedelta(minutes=5)
    current_start = start_time

    while current_start < end_time:
        current_end = current_start + interval_window
        params = {
            "session_key": session_key,
            "date>": current_start.isoformat(),
            "date<": current_end.isoformat()
        }
        
        try:
            response = requests.get("https://api.openf1.org/v1/intervals", params=params, timeout=20)
            
            if response.status_code == 429:
                print(f"🛑 Rate Limited (429). Sleeping 40s...")
                time.sleep(40)
                continue

            if response.status_code == 404:
                current_start = current_end
                continue

            response.raise_for_status()
            data = response.json()

            if data:
                for entry in data:
                    entry['recorded_at'] = entry.pop('date')
                insert_from_dicts("raw.intervals", data)
                print(f"✅ Saved {len(data)} intervals grid-wide [{current_start.strftime('%H:%M')}]")
            
            current_start = current_end
            time.sleep(3)

        except requests.exceptions.RequestException as e:
            print(f"⚠️ API Error at {current_start}: {e}")
            time.sleep(5)
            current_start = current_end

    print(f"🏁 Intervals complete for session {session_key}")

with DAG(
    'f1_ingest_intervals_staging',
    default_args=default_args,
    description='Full pipeline: API -> Postgres -> Parquet',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'automated'],
) as dag:

    # Ingest from API to Postgres
    task_ingest_api = PythonOperator(
        task_id='ingest_cars_api_to_postgres',
        python_callable=ingest_intervals_from_api,
    )

    # Export from Postgres to Parquet
    task_export_parquet = BashOperator(
        task_id='export_postgres_to_parquet',
        bash_command='python3 /opt/airflow/scripts/export_to_parquet.py',
    )

    task_ingest_api >> task_export_parquet