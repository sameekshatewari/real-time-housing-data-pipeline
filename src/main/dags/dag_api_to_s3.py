"""Fetch current weather, publish it to Kafka, then persist that exact offset to S3."""

import sys
from pathlib import Path
from datetime import timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent))
from airflow import DAG

from pipeline_dag_factory import (
    DEFAULTS,
    make_script_task,
    variable_argument,
    xcom_argument,
)


with DAG(
    dag_id="dag_api_to_s3",
    description="Fetch weather, publish it to Kafka, and write that record to S3 CSV.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    tags=["kafksapi", "api", "kafka", "s3"],
) as dag:
    publish_weather = make_script_task(
        dag,
        "fetch_weather_and_publish_to_kafka",
        "src/main/kafka_ingestion.py",
        [
            variable_argument("kafksapi_city", DEFAULTS["kafksapi_city"]),
            "--bootstrap-server",
            variable_argument(
                "kafksapi_kafka_bootstrap_servers",
                DEFAULTS["kafksapi_kafka_bootstrap_servers"],
            ),
        ],
        capture_kafka_metadata=True,
        execution_timeout=timedelta(minutes=2),
    )

    persist_to_s3 = make_script_task(
        dag,
        "persist_published_record_to_s3_csv",
        "src/main/kafka_to_s3_raw_csv.py",
        [
            "--bootstrap-server",
            variable_argument(
                "kafksapi_kafka_bootstrap_servers",
                DEFAULTS["kafksapi_kafka_bootstrap_servers"],
            ),
            "--topic",
            variable_argument(
                "kafksapi_kafka_topic",
                DEFAULTS["kafksapi_kafka_topic"],
            ),
            "--endpoint-url",
            variable_argument(
                "kafksapi_s3_endpoint_url",
                DEFAULTS["kafksapi_s3_endpoint_url"],
            ),
            "--bucket",
            variable_argument(
                "kafksapi_s3_bucket",
                DEFAULTS["kafksapi_s3_bucket"],
            ),
            "--group-id",
            variable_argument(
                "kafksapi_s3_group_id",
                DEFAULTS["kafksapi_s3_group_id"],
            ),
            "--batch-size",
            "1",
            "--max-messages",
            "1",
            "--idle-timeout-seconds",
            variable_argument(
                "kafksapi_idle_timeout_seconds",
                DEFAULTS["kafksapi_idle_timeout_seconds"],
            ),
            "--partition",
            xcom_argument("fetch_weather_and_publish_to_kafka", "partition"),
            "--starting-offset",
            xcom_argument("fetch_weather_and_publish_to_kafka", "offset"),
            "--require-messages",
        ],
        execution_timeout=timedelta(minutes=8),
    )

    publish_weather >> persist_to_s3
