from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='00_test_dbt_connection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    test_dbt = BashOperator(
        task_id='dbt_debug_task',
        bash_command='dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt'
    )

    test_dbt
