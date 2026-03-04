"""
Medallion Pipeline DAG
======================
Orchestrates end-to-end data flow:
  Faker → MinIO (bronze) → PostgreSQL bronze → silver → gold → Metabase

Schedule: every 30 minutes
Alerts:   email on failure and when no new data is detected
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

# faker_to_minio lives in the dags folder alongside this file
from faker_to_minio import generate_and_upload

# data_processor and data_validation live in /opt/airflow/src (on PYTHONPATH)
from src.data_processor import (
    init_medallion_schemas,
    load_bronze_placeholder,
    transform_bronze_to_silver,
    aggregate_silver_to_gold,
)
from src.data_validation import validate_row_counts


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _send_email(subject: str, body: str):
    """Simple wrapper so we can reuse email sending."""
    logger.info("Sending email with subject: %s", subject)
    send_email(to=["dosimeyolivia98@gmail.com"], subject=subject, html_content=body)


def run_data_generation(num_rows: int = 100) -> str:
    """
    Generate fake sales data and upload to MinIO.
    Returns the MinIO object name that was created.
    """
    logger.info("Running data generation for %s rows", num_rows)
    object_name = generate_and_upload(num_rows=num_rows)
    logger.info("Data generation complete. Object name: %s", object_name)
    return object_name


def detect_new_files(**context) -> bool:
    """
    Beginner-friendly 'detection': if data_generator produced an object_name,
    we treat that as 'new data detected'. If not, send an email alert and
    short-circuit the rest of the pipeline for this run.
    """
    object_name = context["ti"].xcom_pull(task_ids="data_generator")
    has_new_data = bool(object_name)
    if not has_new_data:
        logger.warning("No new data detected in this run.")
        _send_email(
            subject="[Mini Data Platform] No new data detected",
            body="<p>No new data detected for this Airflow run. Pipeline stopped.</p>",
        )
    else:
        logger.info("New data detected: %s", object_name)
    return has_new_data


def bronze_loader_task(**context) -> None:
    """Thin wrapper to call the bronze loader from src.data_processor."""
    object_name = context["ti"].xcom_pull(task_ids="data_generator")
    if not object_name:
        raise ValueError("No object_name from data_generator task")
    load_bronze_placeholder(object_name=object_name)


def validation_task() -> None:
    """
    Call validation helpers from src.data_validation.
    If validation fails, raise an exception so the task is retried and
    eventually marked as failed.
    """
    ok = validate_row_counts()
    if not ok:
        raise ValueError("Data validation failed. See logs for details.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["dosimeyolivia98@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="Medallion architecture: Faker -> MinIO -> Bronze -> Silver -> Gold -> Metabase",
    schedule_interval="*/30 * * * *",  # every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    init_schemas = PythonOperator(
        task_id="init_medallion_schemas",
        python_callable=init_medallion_schemas,
        retries=2,
    )

    generate_data = PythonOperator(
        task_id="data_generator",
        python_callable=run_data_generation,
        op_kwargs={"num_rows": 100},
        retries=2,
    )

    detect_new = ShortCircuitOperator(
        task_id="detect_new_files",
        python_callable=detect_new_files,
    )

    process_bronze = PythonOperator(
        task_id="data_processing_bronze",
        python_callable=bronze_loader_task,
    )

    process_silver = PythonOperator(
        task_id="data_processing_silver",
        python_callable=transform_bronze_to_silver,
    )

    process_gold = PythonOperator(
        task_id="data_processing_gold",
        python_callable=aggregate_silver_to_gold,
    )

    validate_data = PythonOperator(
        task_id="data_validation",
        python_callable=validation_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Task dependencies (linear chain)
    (
        init_schemas
        >> generate_data
        >> detect_new
        >> process_bronze
        >> process_silver
        >> process_gold
        >> validate_data
    )
