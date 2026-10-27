"""Airflow DAG entry point for the Hudi-to-PostgreSQL loader."""

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
    dag_id="dag_s3_hudi_to_dbload",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "spark", "postgres"],
) as dag:
    make_script_task(
        dag,
        "run_job",
        "src/main/s3_hudi_to_dbload.py",
        [
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
            "--pg-host",
            variable_argument(
                "kafksapi_postgres_host",
                DEFAULTS["kafksapi_postgres_host"],
            ),
            "--pg-port",
            variable_argument(
                "kafksapi_postgres_port",
                DEFAULTS["kafksapi_postgres_port"],
            ),
            "--pg-database",
            variable_argument(
                "kafksapi_postgres_database",
                DEFAULTS["kafksapi_postgres_database"],
            ),
            "--pg-user",
            variable_argument(
                "kafksapi_postgres_user",
                DEFAULTS["kafksapi_postgres_user"],
            ),
        ],
        spark_job=True,
        requires_db_password=True,
    )
