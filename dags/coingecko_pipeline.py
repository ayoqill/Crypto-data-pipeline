# Daily orchestration of CoinGecko data pipeline
# Real pipeline logic is inside /scripts
# This DAG only calls those scripts

import os
import sys
import logging
from pathlib import Path
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# If you want to keep PostgresOperator (works, but deprecated warning):
from airflow.providers.postgres.operators.postgres import PostgresOperator

# --- Make /opt/airflow (project root) importable so `scripts.*` works ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # .../CRYPTO-DATA-PIPELINE
sys.path.append(str(PROJECT_ROOT))

from scripts.extract import fetch_market_data
from scripts.transform import transform_market_json
from scripts.load import upsert_daily
from scripts.storage_minio import ensure_bucket, put_json, get_json

logger = logging.getLogger(__name__)

BUCKET = "coingecko-raw"
COINS = ["bitcoin", "ethereum", "solana", "tether", "bonk"]

default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# --------------------------
# Task callables
# --------------------------
def task_ensure_minio_bucket():
    ensure_bucket(BUCKET)
    logger.info("MinIO bucket ensured: %s", BUCKET)


def task_extract_and_store_raw(**context):
    payload, meta = fetch_market_data(COINS, vs_currency="usd")

    logical_date = context["logical_date"].date()
    object_name = f"raw/coingecko/dt={logical_date.isoformat()}/market.json"
    meta_name = f"raw/coingecko/dt={logical_date.isoformat()}/meta.json"

    put_json(BUCKET, object_name, payload)
    put_json(BUCKET, meta_name, meta)

    logger.info("Stored market.json: %s/%s", BUCKET, object_name)
    logger.info("Stored meta.json: %s/%s", BUCKET, meta_name)
    logger.info("API status: %s", meta.get("status_code"))

    # pass only the MinIO object path to next task
    context["ti"].xcom_push(key="market_object", value=object_name)


def task_transform_and_load(**context):
    logical_date = context["logical_date"].date()

    market_object = context["ti"].xcom_pull(
        key="market_object",
        task_ids="extract_store_raw",
    )
    if not market_object:
        raise RuntimeError("market_object not found in XCom. Extract task may have failed.")

    logger.info("Loading raw payload from MinIO: %s/%s", BUCKET, market_object)
    raw_payload = get_json(BUCKET, market_object)

    rows = transform_market_json(raw_payload, snapshot_date=logical_date)
    count = upsert_daily(rows)

    logger.info("Upsert completed. rows_upserted=%s", count)
    context["ti"].xcom_push(key="rows_upserted", value=count)

    if count == 0:
        raise RuntimeError("No rows loaded. API returned empty or transform failed.")


# --------------------------
# DAG definition
# --------------------------
with DAG(
    dag_id="coingecko_daily_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["coingecko", "crypto", "portfolio"],
    # IMPORTANT: lets Airflow find your SQL file at PROJECT_ROOT/include/create_tables.sql
    template_searchpath=[str(PROJECT_ROOT)],
) as dag:

    start = EmptyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        # This path is relative to template_searchpath
        sql="include/create_tables.sql",
    )

    ensure_bucket_task = PythonOperator(
        task_id="ensure_minio_bucket",
        python_callable=task_ensure_minio_bucket,
    )

    extract_store_raw = PythonOperator(
        task_id="extract_store_raw",
        python_callable=task_extract_and_store_raw,
    )

    transform_load = PythonOperator(
        task_id="transform_load",
        python_callable=task_transform_and_load,
    )

    end = EmptyOperator(task_id="end")

    start >> create_table >> ensure_bucket_task >> extract_store_raw >> transform_load >> end
