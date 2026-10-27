"""Airflow DAG entry point for non-destructive pipeline checks."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from airflow import DAG

from pipeline_dag_factory import (
    make_script_task,
)


with DAG(
    dag_id="dag_pipeline_preflight",
    schedule=None,
    catchup=False,
    tags=["kafksapi", "preflight"],
) as dag:
    make_script_task(
        dag,
        "run_checks",
        "test/test_pipeline_preflight.py",
        spark_job=True,
        requires_db_password=True,
    )
