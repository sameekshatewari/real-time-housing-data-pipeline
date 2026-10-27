# Pipeline and Airflow architecture

## Data flow

```text
Weather API
    |
    v
Kafka topic: cityweather
    |                         |
    |                         +--> local CSV files: data/input/
    |                         |
    |                         +--> S3 CSV: raw/csv/weather.csv
    |
    +--> Spark Structured Streaming --> raw Hudi: raw/parquet
                                         |
                                         v
                                  curated Hudi: curated/weather
                                         |
                                         v
                                  PostgreSQL: weather.weather_data
```

Kafka, S3-compatible storage, and PostgreSQL are external services. Airflow
orchestrates the scripts; it does not import their implementation modules.

## Script and DAG inventory

| Script | Responsibility | Airflow entry |
|---|---|---|
| `src/main/weather_api.py` | Fetches weather JSON; used by the Kafka producer. | Library; no separate DAG. |
| `src/main/kafka_ingestion.py` | Fetches weather and publishes one record (or repeats with `--interval`) to Kafka. | `dag_kafka_ingestion.py` |
| `src/main/kafka_to_csv.py` | Reads Kafka with a consumer group and writes one message per local CSV file in `data/input/`. | `dag_kafka_to_csv.py` |
| `src/main/kafka_to_s3_raw_csv.py` | Consumes Kafka records and upserts them into a raw S3 CSV. | `dag_kafka_to_s3_raw_csv.py` |
| `src/main/kafka_to_s3_raw_parequet.py` | Spark Structured Streaming from Kafka into a raw Hudi table on S3. Continuous mode is for the Windows service scripts; `--available-now` is finite for Airflow. | `dag_kafka_to_s3_raw_parequet.py` |
| `src/main/s3_raw_to_curate_hudi.py` | Transforms raw Hudi records into curated Hudi records. | `dag_s3_raw_to_curate_hudi.py` |
| `src/main/s3_hudi_to_dbload.py` | Upserts curated Hudi rows into PostgreSQL. | `dag_s3_hudi_to_dbload.py` |
| API-to-S3 workflow | Fetches current weather, publishes to Kafka, then stores that exact Kafka offset in the raw S3 CSV. | `dag_api_to_s3.py` |
| `test/test.py` | Kafka producer/consumer integration test. | Run manually; it publishes a weather message. |
| `test/test_pipeline_preflight.py` | Non-destructive Kafka, S3, PostgreSQL connectivity, local CSV, and local Hudi checks. | `dag_pipeline_preflight.py` |

Airflow DAG files live in `src/main/dags/` and use the
`dag_<script>.py` naming convention. They are manual-trigger DAGs by default,
so no pipeline writes occur just because the scheduler starts. The local CSV
and S3 CSV DAGs use finite batches; idle timeouts let an empty batch finish.
The Kafka-to-Hudi DAG calls
`src/scripts/run_kafka_to_s3_raw_parequet.sh`, which starts the Spark job as a
child process using the `kafksapi_spark_python` Airflow Variable. The script passes `--available-now`
so the DAG task exits after processing currently available records. Use the
PowerShell start/stop scripts for an always-on Windows host stream instead.

## Runtime separation

Each DAG contains only Airflow operators and command construction. Tasks
start scripts as separate processes. Spark/Hudi imports,
Spark session creation, Maven package resolution, and Spark shutdown happen
only when that task process runs; they do not happen while Airflow parses a
DAG.

This still requires an appropriate job runtime when a task executes:

- The regular Python runtime needs `kafka-python`, `boto3`, and `requests`.
- The Spark runtime needs Python, PySpark 3.5.9, Java, and access to Maven
  Central on its first launch.
- Kafka, S3, and PostgreSQL addresses must be reachable from the task runtime.
- The database loader reads the Airflow Variable `db_password` at task runtime
  and passes it through the child process environment.

Set the Airflow Variables below; DAG tasks read them at execution time rather
than embedding configuration in DAG files. `kafksapi_spark_python` may point
to a dedicated Python environment so the Airflow DAG parser and ordinary task
runtime do not need Spark/Hudi installed. The code directory must be mounted
at `kafksapi_project_dir` (default `/opt/airflow/project`).
The Windows `.venv` cannot be used inside the Linux Airflow container; create
a Linux runtime for the task process.

The currently running `festive_colden` Airflow container has no project mount,
and its Python environment does not include `kafka-python` or PySpark. Copying
DAG files into it makes them discoverable but does not make task runtimes
available. For durable deployment, start Airflow with bind mounts for both
`src/main/dags` at `/opt/airflow/dags` and the project at
`/opt/airflow/project`, then provide the runtime executables and environment
variables to the task workers. When running inside Docker Desktop, service
addresses commonly use `host.docker.internal`; verify Kafka's advertised
listener is also reachable from that container, not just its published
bootstrap port.

Airflow Variables used by the DAGs:

```text
kafksapi_project_dir=/opt/airflow/project
kafksapi_python=/path/to/linux/python
kafksapi_spark_python=/path/to/spark/python
kafksapi_kafka_bootstrap_servers=host.docker.internal:9092
kafksapi_kafka_topic=cityweather
kafksapi_s3_endpoint_url=http://host.docker.internal:4566
kafksapi_s3_bucket=s3-kadka-api-raw
kafksapi_aws_access_key_id=test
kafksapi_aws_secret_access_key=test
kafksapi_aws_region=us-east-1
kafksapi_postgres_host=host.docker.internal
kafksapi_postgres_port=5432
kafksapi_postgres_database=weather_db
kafksapi_postgres_user=batch
kafksapi_city=Dallas
kafksapi_batch_size=100
kafksapi_idle_timeout_seconds=10
kafksapi_s3_group_id=cityweather-api-to-s3
db_password=<actual PostgreSQL password, stored as an Airflow Variable>
```

The `db_password` Airflow Variable is read only when the database or full
preflight task runs. It is not embedded in DAG code, command arguments, or
logs. Set it to the actual database password through the Airflow UI or a
protected secret-management process; do not commit it. The AWS values shown
are LocalStack placeholders, not real AWS credentials. The preflight test
only checks the configured bucket and does not create buckets or change
objects.

## Start and stop the continuous Hudi stream on Windows

From the project root, run the PowerShell wrappers:

```powershell
.\src\scripts\start_kafka_to_s3_raw_parequet.ps1
.\src\scripts\stop_kafka_to_s3_raw_parequet.ps1
```

The matching shell runner, `src/scripts/run_kafka_to_s3_raw_parequet.sh`, is
for Linux Airflow tasks and is called by the finite Airflow DAG; it is not a
daemon. The PowerShell start script verifies the project virtual environment,
prevents a second copy from starting, and writes stdout/stderr logs and a PID
file under `data/processes/`. The stop script verifies the saved PID belongs
to this job before stopping its process tree. This host-managed stream is
separate from Airflow DAG runs; the DAG handles bounded `--available-now`
processing.

## Checks

Run the non-destructive preflight on the machine that has network access to
the services:

```powershell
.\.venv\Scripts\python.exe .\test\test_pipeline_preflight.py
```

It verifies the Kafka topic, checks the existing S3 bucket without writing,
checks PostgreSQL TCP connectivity, exercises the local CSV writer in a
temporary directory, and writes/reads a temporary local Hudi table. Set
`WEATHER_DB_PASSWORD` to additionally verify PostgreSQL JDBC credentials with
a read-only `SELECT 1`. It does not validate the target table schema or write
to the configured S3 bucket.
