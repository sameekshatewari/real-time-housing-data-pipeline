"""Airflow DAG entry point for raw S3 CSV output."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from airflow import DAG

from pipeline_dag_factory import (
    DEFAULTS,
    make_script_task,
    variable_argument,
)


with DAG(
    dag_id="dag_kafka_to_s3_raw_csv",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "pipeline"],
) as dag:
    make_script_task(
        dag,
        "run_job",
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
            variable_argument(
                "kafksapi_batch_size",
                DEFAULTS["kafksapi_batch_size"],
            ),
            "--max-messages",
            variable_argument("kafksapi_batch_size", DEFAULTS["kafksapi_batch_size"]),
            "--idle-timeout-seconds",
            variable_argument(
                "kafksapi_idle_timeout_seconds",
                DEFAULTS["kafksapi_idle_timeout_seconds"],
            ),
        ],
    )
