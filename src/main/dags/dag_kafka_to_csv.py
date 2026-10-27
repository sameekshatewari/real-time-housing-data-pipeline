"""Airflow DAG entry point for local CSV output."""

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
    dag_id="dag_kafka_to_csv",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "pipeline"],
) as dag:
    make_script_task(
        dag,
        "run_job",
        "src/main/kafka_to_csv.py",
        [
            "--bootstrap-server",
            variable_argument(
                "kafksapi_kafka_bootstrap_servers",
                DEFAULTS["kafksapi_kafka_bootstrap_servers"],
            ),
            "--max-messages",
            variable_argument(
                "kafksapi_batch_size",
                DEFAULTS["kafksapi_batch_size"],
            ),
            "--idle-timeout-seconds",
            variable_argument(
                "kafksapi_idle_timeout_seconds",
                DEFAULTS["kafksapi_idle_timeout_seconds"],
            ),
            "--output-dir",
            variable_argument(
                "kafksapi_local_csv_dir",
                "/opt/airflow/project/data/input",
            ),
        ],
    )
