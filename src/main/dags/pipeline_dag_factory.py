import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context


PROJECT_DIR_VARIABLE = "kafksapi_project_dir"
DEFAULT_PROJECT_DIR = "/opt/airflow/project"
DEFAULTS = {
    "kafksapi_python": sys.executable,
    "kafksapi_spark_python": "python",
    "kafksapi_kafka_bootstrap_servers": "host.docker.internal:9092",
    "kafksapi_s3_endpoint_url": "http://host.docker.internal:4566",
    "kafksapi_s3_bucket": "s3-kadka-api-raw",
    "kafksapi_aws_access_key_id": "test",
    "kafksapi_aws_secret_access_key": "test",
    "kafksapi_aws_region": "us-east-1",
    "kafksapi_postgres_host": "host.docker.internal",
    "kafksapi_postgres_port": "5432",
    "kafksapi_postgres_database": "weather_db",
    "kafksapi_postgres_user": "batch",
    "kafksapi_city": "Dallas",
    "kafksapi_kafka_topic": "cityweather",
    "kafksapi_batch_size": "100",
    "kafksapi_idle_timeout_seconds": "10",
    "kafksapi_s3_group_id": "cityweather-api-to-s3",
}


def variable_argument(key, default=None, required=False):
    return {
        "variable": key,
        "default": default,
        "required": required,
    }


def xcom_argument(task_id, key):
    return {"xcom_task_id": task_id, "xcom_key": key}


def _read_variable(key, default=None, required=False):
    value = Variable.get(key, default_var=default)
    if required and not value:
        raise AirflowException(
            f"Set the required Airflow Variable {key!r} before running this task."
        )
    return value


def _resolve_argument(argument, context):
    if isinstance(argument, dict) and "variable" in argument:
        return _read_variable(
            argument["variable"],
            default=argument.get("default"),
            required=argument.get("required", False),
        )
    if isinstance(argument, dict) and "xcom_task_id" in argument:
        result = context["ti"].xcom_pull(task_ids=argument["xcom_task_id"])
        if not isinstance(result, dict) or argument["xcom_key"] not in result:
            raise AirflowException(
                f"Task {argument['xcom_task_id']!r} did not provide "
                f"{argument['xcom_key']!r}."
            )
        return str(result[argument["xcom_key"]])
    return str(argument)


def run_external_script(
    script_path,
    arguments=(),
    *,
    spark_job=False,
    shell_script=False,
    requires_db_password=False,
    capture_kafka_metadata=False,
):
    context = get_current_context()
    project_dir = Path(
        _read_variable(PROJECT_DIR_VARIABLE, DEFAULT_PROJECT_DIR)
    ).resolve()
    executable = (
        "bash"
        if shell_script
        else _read_variable(
            "kafksapi_spark_python" if spark_job else "kafksapi_python",
            DEFAULTS["kafksapi_spark_python" if spark_job else "kafksapi_python"],
        )
    )
    script = project_dir.joinpath(*PurePosixPath(script_path).parts)
    if not script.is_file():
        raise AirflowException(
            f"Pipeline script was not found: {script}. "
            f"Mount the project at Airflow Variable {PROJECT_DIR_VARIABLE!r}."
        )

    command = [executable, str(script)]
    command.extend(_resolve_argument(arg, context) for arg in arguments)
    environment = os.environ.copy()
    environment["KAFKSAPI_PROJECT_DIR"] = str(project_dir)
    environment["KAFKSAPI_SPARK_PYTHON"] = _read_variable(
        "kafksapi_spark_python",
        DEFAULTS["kafksapi_spark_python"],
    )
    environment["KAFKSAPI_KAFKA_BOOTSTRAP_SERVERS"] = _read_variable(
        "kafksapi_kafka_bootstrap_servers",
        DEFAULTS["kafksapi_kafka_bootstrap_servers"],
    )
    environment["KAFKA_BOOTSTRAP_SERVERS"] = environment[
        "KAFKSAPI_KAFKA_BOOTSTRAP_SERVERS"
    ]
    environment["AWS_ENDPOINT_URL"] = _read_variable(
        "kafksapi_s3_endpoint_url",
        DEFAULTS["kafksapi_s3_endpoint_url"],
    )
    environment["AWS_ACCESS_KEY_ID"] = _read_variable(
        "kafksapi_aws_access_key_id",
        DEFAULTS["kafksapi_aws_access_key_id"],
    )
    environment["AWS_SECRET_ACCESS_KEY"] = _read_variable(
        "kafksapi_aws_secret_access_key",
        DEFAULTS["kafksapi_aws_secret_access_key"],
    )
    environment["AWS_DEFAULT_REGION"] = _read_variable(
        "kafksapi_aws_region",
        DEFAULTS["kafksapi_aws_region"],
    )
    environment["S3_BUCKET"] = _read_variable(
        "kafksapi_s3_bucket",
        DEFAULTS["kafksapi_s3_bucket"],
    )
    environment["WEATHER_DB_HOST"] = _read_variable(
        "kafksapi_postgres_host",
        DEFAULTS["kafksapi_postgres_host"],
    )
    environment["WEATHER_DB_PORT"] = _read_variable(
        "kafksapi_postgres_port",
        DEFAULTS["kafksapi_postgres_port"],
    )
    environment["WEATHER_DB_NAME"] = _read_variable(
        "kafksapi_postgres_database",
        DEFAULTS["kafksapi_postgres_database"],
    )
    environment["WEATHER_DB_USER"] = _read_variable(
        "kafksapi_postgres_user",
        DEFAULTS["kafksapi_postgres_user"],
    )
    if requires_db_password:
        environment["WEATHER_DB_PASSWORD"] = _read_variable(
            "db_password",
            required=True,
        )

    result = subprocess.run(
        command,
        cwd=project_dir,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="", flush=True)
    if result.returncode:
        raise AirflowException(
            f"Pipeline process exited with status {result.returncode}: {script}"
        )

    if capture_kafka_metadata:
        matches = re.findall(
            r"Published successfully: topic=([^,\r\n]+), "
            r"partition=(\d+), offset=(\d+)",
            result.stdout,
        )
        if not matches:
            raise AirflowException(
                "Kafka producer succeeded but did not report its topic, "
                "partition, and offset."
            )
        topic, partition, offset = matches[-1]
        return {
            "topic": topic,
            "partition": int(partition),
            "offset": int(offset),
        }
    return None


def make_script_task(
    dag,
    task_id,
    script_path,
    arguments=(),
    *,
    spark_job=False,
    shell_script=False,
    requires_db_password=False,
    capture_kafka_metadata=False,
    execution_timeout=timedelta(hours=3),
):
    return PythonOperator(
        task_id=task_id,
        python_callable=run_external_script,
        op_kwargs={
            "script_path": script_path,
            "arguments": arguments,
            "spark_job": spark_job,
            "shell_script": shell_script,
            "requires_db_password": requires_db_password,
            "capture_kafka_metadata": capture_kafka_metadata,
        },
        dag=dag,
        do_xcom_push=capture_kafka_metadata,
        show_return_value_in_logs=False,
        execution_timeout=execution_timeout,
    )


def make_script_dag(
    dag_id,
    script_path,
    arguments=(),
    *,
    spark_job=False,
    shell_script=False,
    requires_db_password=False,
):
    with DAG(
        dag_id=dag_id,
        description=f"Run {script_path} as an external pipeline process.",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=3),
        default_args={"owner": "airflow", "retries": 0},
        tags=["kafksapi", "spark" if spark_job else "pipeline"],
    ) as dag:
        make_script_task(
            dag,
            "run_job",
            script_path,
            arguments,
            spark_job=spark_job,
            shell_script=shell_script,
            requires_db_password=requires_db_password,
        )
    return dag
