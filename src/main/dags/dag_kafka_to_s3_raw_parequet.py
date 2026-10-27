"""Airflow DAG entry point for a finite Kafka-to-Hudi stream run."""

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
    dag_id="dag_kafka_to_s3_raw_parequet",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "spark"],
) as dag:
    make_script_task(
        dag,
        "run_job",
        "src/scripts/run_kafka_to_s3_raw_parequet.sh",
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
            "--available-now",
        ],
        spark_job=True,
        shell_script=True,
    )
