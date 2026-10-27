"""Airflow DAG entry point for raw-to-curated Hudi processing."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from airflow import DAG

from pipeline_dag_factory import DEFAULTS, make_script_task, variable_argument


with DAG(
    dag_id="dag_s3_raw_to_curate_hudi",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "spark"],
) as dag:
    make_script_task(
        dag,
        "run_job",
        "src/main/s3_raw_to_curate_hudi.py",
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
        ],
        spark_job=True,
    )
