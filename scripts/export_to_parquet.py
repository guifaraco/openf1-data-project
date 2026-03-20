import duckdb
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def get_pg_conn_str():
    """
    Generates the PostgreSQL connection string.
    Automatically detects if running inside Docker or on the Host.
    """
    is_docker = os.path.exists('/.dockerenv')
    db_host = "postgres" if is_docker else "localhost"
    
    return (
        f"host={db_host} port=5432 dbname={os.getenv('POSTGRES_DB')} "
        f"user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}"
    )

def export_table_to_parquet(table_name, file_name):
    """
    Generic function to export any Postgres table to a Parquet file.
    """
    conn_str = get_pg_conn_str()
    con = duckdb.connect()
    
    # Ensure extensions are available
    con.execute("INSTALL postgres; LOAD postgres;")
    
    output_path = Path(__file__).parent.parent / "data" / f"{file_name}.parquet"
    
    print(f"Exporting {table_name} to {output_path}...")
    
    query = f"""
        COPY (
            SELECT * FROM postgres_scan('{conn_str}', 'raw', '{table_name}')
        ) TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY');
    """
    
    con.execute(query)
    print(f"✅ Success: {table_name} exported.")

if __name__ == "__main__":
    # export_table_to_parquet('drivers', 'raw_drivers')
    print(env_path)