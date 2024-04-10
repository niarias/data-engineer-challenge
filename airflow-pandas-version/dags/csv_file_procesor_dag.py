from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from scripts.main import procesar_archivo



with DAG(
    dag_id="process_csv_file_dag",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *"
) as dag:
    dummy_start_task = DummyOperator(
        task_id="dummy_seed_start"
    )

    read_csv_file = PythonOperator(
        task_id="read_and_process_csv_file",
        python_callable=procesar_archivo,
        op_kwargs={
            "params": {
                "path": "scripts/data/config/config.json",
            }
        }
    )


    dummy_end_task = DummyOperator(
        task_id="dummy_seed_end_task"
    )

    dummy_start_task >> read_csv_file >> dummy_end_task