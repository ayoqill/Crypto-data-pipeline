# Daily orchestration of Coingecko data pipeline
# real pipeline logic is at scripts folder
# just use this file to call the scripts here

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timezone, date

from scripts.extract import fetch_market_data
from scripts.transform import transform_market_json
from scripts.load import upsert_daily
from scripts.storage_minio import ensure_bucket, put_json

BUCKET = "coingecko-raw"
COINS = ["bitcoin", "ethereum", "solana", "ripple", "cardano"]

default_args = {"owner": "student", "retries": 1}

def task_ensure_minio_bucket():
    ensure_bucket(BUCKET)

def task_extract_and_store_raw(**context):
    payload, meta = fetch_market_data(COINS, vs_currency="usd")

    # Partition by run date (Airflow logical date)
    logical_date = context["logical_date"].date()
    object_name = f"raw/coingecko/dt={logical_date.isoformat()}/market.json"
    meta_name = f"raw/coingecko/dt={logical_date.isoformat()}/meta.json"

    put_json(BUCKET, object_name, payload)
    put_json(BUCKET, meta_name, meta)

    # pass raw data via XCom (fine for small payload; for big, store only paths)
    context["ti"].xcom_push(key="raw_payload", value=payload)

def task_transform_and_load(**context):
    logical_date = context["logical_date"].date()
    raw_payload = context["ti"].xcom_pull(key="raw_payload", task_ids="extract_store_raw")

    rows = transform_market_json(raw_payload, snapshot_date=logical_date)
    count = upsert_daily(rows)

    # simple monitoring metric
    context["ti"].xcom_push(key="rows_upserted", value=count)
    if count == 0:
        raise RuntimeError("No rows loaded. Something is wrong (API empty / transform issue).")

with DAG(
    dag_id="coingecko_daily_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["coingecko", "crypto", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Create table if not exists
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql="include/create_tables.sql",
    )

    ensure_bucket_task = PythonOperator(
        task_id="ensure_minio_bucket",
        python_callable=task_ensure_minio_bucket,
    )

    extract_store_raw = PythonOperator(
        task_id="extract_store_raw",
        python_callable=task_extract_and_store_raw,
        provide_context=True,
    )

    transform_load = PythonOperator(
        task_id="transform_load",
        python_callable=task_transform_and_load,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> create_table >> ensure_bucket_task >> extract_store_raw >> transform_load >> end
