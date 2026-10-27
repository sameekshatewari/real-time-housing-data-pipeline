"""Airflow DAG entry point for the weather Kafka producer."""

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
    dag_id="dag_kafka_ingestion",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "pipeline"],
) as dag:
    make_script_task(
        dag,
        "publish_weather",
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
    )
