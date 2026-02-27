import psycopg2
import os
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def get_connection():
    """
    Establishes a connection to the PostgreSQL database.
    Note: In Airflow, you'll eventually replace this with PostgresHook.
    """
    db_host = os.getenv("POSTGRES_HOST", "localhost")

    return psycopg2.connect(
        host=db_host,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )

def setup_infrastructure():
    """
    Creates the schema and tables needed for raw ingestion.
    """
    queries = [
        "CREATE SCHEMA IF NOT EXISTS raw;",
        """
        CREATE TABLE IF NOT EXISTS raw.drivers (
            driver_number INT,
            broadcast_name VARCHAR(100),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            headshot_url VARCHAR(256),
            full_name VARCHAR(100),
            name_acronym VARCHAR(10),
            team_name VARCHAR(100),
            team_colour VARCHAR(50),
            session_key INT,
            meeting_key INT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.cars (
            brake INT,
            recorded_at TIMESTAMP,
            driver_number INT,
            drs INT,
            meeting_key INT,
            n_gear INT,
            rpm INT,
            session_key INT,
            speed INT,
            throttle INT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.intervals (
            recorded_at TIMESTAMP,
            driver_number INT,
            gap_to_leader DECIMAL,
            interval DECIMAL,
            meeting_key INT,
            session_key INT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    ]
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        for q in queries:
            cur.execute(q)
        conn.commit()
        print("✅ Database infrastructure is ready.")
    except Exception as e:
        print(f"❌ Setup error: {e}")
    finally:
        if conn: conn.close()

def insert_from_dicts(table_name, data_dicts, batch_size=5000, delete_key=None):
    """
    table_name: 'raw.cars'
    data_dicts: List of dictionaries from API
    delete_key: A tuple of (column_name, value), e.g., ('session_key', 9506)
    """
    if not data_dicts:
        print("⚠️ No data provided.")
        return

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # SOFT TRUNCATE
        if delete_key:
            col, val = delete_key
            print(f"🧹 Cleaning up {table_name} for {col}={val}...")
            # %s for safety against SQL injection
            cur.execute(f"DELETE FROM {table_name} WHERE {col} = %s", (val,))

        # INSERT STEP
        columns = list(data_dicts[0].keys())
        cols_with_meta = columns + ["ingested_at"]
        now = datetime.now()
        data_rows = [tuple(d[col] for col in columns) + (now,) for d in data_dicts]

        query = f"INSERT INTO {table_name} ({', '.join(cols_with_meta)}) VALUES %s"
        
        for i in range(0, len(data_rows), batch_size):
            chunk = data_rows[i : i + batch_size]
            execute_values(cur, query, chunk)
        
        conn.commit()
        print(f"✅ Successfully refreshed {len(data_rows)} rows in {table_name}.")
        
    except Exception as e:
        print(f"❌ Error: {e}. Rolling back.")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    print("Starting database infrastructure setup...")
    setup_infrastructure()
    print("Infrastructure setup finished.")