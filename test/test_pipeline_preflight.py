import csv
import importlib.util
import json
import os
import socket
import sys
import tempfile
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError


PROJECT_DIR = Path(__file__).resolve().parents[1]
MAIN_DIR = PROJECT_DIR / "src" / "main"


def load_local_csv_writer():
    module_path = MAIN_DIR / "kafka_to_csv.py"
    spec = importlib.util.spec_from_file_location(
        "kafka_to_csv_preflight",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_kafka():
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:9092",
    )
    expected_topic = os.getenv("KAFKA_TOPIC", "cityweather")
    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="kafksapi-preflight",
            request_timeout_ms=5000,
        )
        topics = admin.list_topics()
        if expected_topic not in topics:
            raise RuntimeError(
                f"Kafka is reachable, but topic {expected_topic!r} is missing."
            )
        return f"reachable; topic {expected_topic!r} exists"
    except (KafkaError, OSError, RuntimeError) as exc:
        raise RuntimeError(f"Kafka check failed: {exc}") from exc
    finally:
        if admin is not None:
            admin.close()


def check_s3():
    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    bucket = os.getenv("S3_BUCKET", "s3-kadka-api-raw")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.getenv(
            "AWS_DEFAULT_REGION",
            os.getenv("AWS_REGION", "us-east-1"),
        ),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        client.head_bucket(Bucket=bucket)
        return f"reachable; bucket {bucket!r} exists (read-only check)"
    except (BotoCoreError, ClientError, OSError) as exc:
        raise RuntimeError(f"S3 check failed: {exc}") from exc
    finally:
        client.close()


def check_postgres_tcp():
    host = os.getenv("WEATHER_DB_HOST", "127.0.0.1")
    port = int(os.getenv("WEATHER_DB_PORT", "5432"))
    try:
        with socket.create_connection((host, port), timeout=3):
            pass
        auth_status = (
            "JDBC authentication is also tested by the Spark/Hudi check"
            if os.getenv("WEATHER_DB_PASSWORD")
            else "JDBC authentication skipped; set WEATHER_DB_PASSWORD to test it"
        )
        return f"TCP reachable at {host}:{port}; {auth_status}"
    except OSError as exc:
        raise RuntimeError(
            f"PostgreSQL is not reachable at {host}:{port}: {exc}"
        ) from exc


def check_local_csv():
    writer = load_local_csv_writer()
    message = json.dumps(
        {"city": "preflight", "temperature_c": 0},
        separators=(",", ":"),
    )
    with tempfile.TemporaryDirectory(prefix="kafksapi-csv-check-") as temp_dir:
        output = writer.save_csv(
            raw_message=message,
            record_number=1,
            run_id="preflight",
            topic="cityweather",
            output_dir=Path(temp_dir),
        )
        with output.open(newline="", encoding="utf-8") as file:
            row = next(csv.DictReader(file))
        if row["kafka_message"] != message:
            raise RuntimeError("The local CSV did not preserve the JSON message.")
    return "wrote and verified a temporary CSV; project data was not changed"


def check_hudi_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(f"PySpark is unavailable: {exc}") from exc

    hudi_version = "1.0.2"
    spark_version = "3.5.9"
    hudi_package = (
        "org.apache.hudi:hudi-spark3.5-bundle_2.12:"
        f"{hudi_version}"
    )
    postgres_package = "org.postgresql:postgresql:42.7.5"
    spark = None
    with tempfile.TemporaryDirectory(prefix="kafksapi-hudi-check-") as temp_dir:
        table_path = (Path(temp_dir) / "hudi-table").as_uri()
        try:
            spark = (
                SparkSession.builder.appName("kafksapi-hudi-preflight")
                .master("local[1]")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.pyspark.python", sys.executable)
                .config("spark.pyspark.driver.python", sys.executable)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config(
                    "spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer",
                )
                .config(
                    "spark.jars.packages",
                    f"{hudi_package},{postgres_package}",
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
                )
                .config(
                    "spark.kryo.registrator",
                    "org.apache.spark.HoodieSparkKryoRegistrar",
                )
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
            frame = spark.sql(
                "SELECT 'preflight' AS record_key, 1 AS check_value"
            )
            (
                frame.write.format("hudi")
                .option("hoodie.table.name", "kafksapi_preflight")
                .option("hoodie.metadata.enable", "false")
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.datasource.write.operation", "upsert")
                .option("hoodie.datasource.write.recordkey.field", "record_key")
                .mode("overwrite")
                .save(table_path)
            )
            count = spark.read.format("hudi").load(table_path).count()
            if count != 1:
                raise RuntimeError(
                    f"Expected one local Hudi row after write; found {count}."
                )
            jdbc_status = ""
            password = os.getenv("WEATHER_DB_PASSWORD")
            if password:
                host = os.getenv("WEATHER_DB_HOST", "127.0.0.1")
                port = os.getenv("WEATHER_DB_PORT", "5432")
                database = os.getenv("WEATHER_DB_NAME", "weather_db")
                user = os.getenv("WEATHER_DB_USER", "batch")
                properties = spark._jvm.java.util.Properties()
                properties.setProperty("user", user)
                properties.setProperty("password", password)
                connection = spark._jvm.java.sql.DriverManager.getConnection(
                    f"jdbc:postgresql://{host}:{port}/{database}",
                    properties,
                )
                try:
                    statement = connection.createStatement()
                    try:
                        result = statement.executeQuery("SELECT 1")
                        try:
                            if not result.next() or result.getInt(1) != 1:
                                raise RuntimeError(
                                    "PostgreSQL SELECT 1 returned no result."
                                )
                        finally:
                            result.close()
                    finally:
                        statement.close()
                finally:
                    connection.close()
                jdbc_status = f"; PostgreSQL JDBC SELECT 1 succeeded at {host}:{port}/{database}"
        except Exception as exc:
            raise RuntimeError(
                "Local Spark/Hudi write-read check failed. It only uses a "
                f"temporary local table: {type(exc).__name__}: {exc}"
            ) from exc
        finally:
            if spark is not None:
                spark.stop()
    return (
        f"Spark {spark_version}/Hudi {hudi_version} wrote and read one "
        "temporary local row; S3 was not accessed"
        f"{jdbc_status}"
    )


def main():
    checks = [
        ("Kafka", check_kafka),
        ("S3", check_s3),
        ("PostgreSQL", check_postgres_tcp),
        ("Local CSV", check_local_csv),
        ("Spark/Hudi", check_hudi_spark),
    ]
    failures = 0
    for label, check in checks:
        try:
            print(f"PASS {label}: {check()}", flush=True)
        except Exception as exc:
            failures += 1
            print(f"FAIL {label}: {exc}", file=sys.stderr, flush=True)
    print(
        f"\nPreflight result: {len(checks) - failures}/{len(checks)} checks passed.",
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
