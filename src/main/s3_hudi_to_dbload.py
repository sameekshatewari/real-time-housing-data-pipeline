import argparse
import os
import sys

from py4j.protocol import Py4JError, Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession


HUDI_VERSION = "1.0.2"
HADOOP_AWS_VERSION = "3.3.4"
POSTGRESQL_JDBC_VERSION = "42.7.5"
DEFAULT_BUCKET = "s3-kadka-api-raw"
TARGET_SCHEMA = "weather"
TARGET_TABLE = "weather_data"
TARGET_COLUMNS = (
    "record_key",
    "kafka_topic",
    "city",
    "region",
    "country",
    "latitude",
    "longitude",
    "weather_date",
    "weather_time",
    "timezone",
    "fetched_at_utc",
    "temperature_c",
    "feels_like_c",
    "humidity_percent",
    "precipitation_mm",
    "wind_speed_kmh",
    "load_date",
)
def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Upsert curated weather Hudi records into PostgreSQL "
            "weather.weather_data."
        )
    )
    parser.add_argument(
        "--endpoint-url",
        default="http://localhost:4566",
        help="S3-compatible endpoint. Default: http://localhost:4566",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"S3 bucket. Default: {DEFAULT_BUCKET}",
    )
    parser.add_argument(
        "--source-path",
        default=None,
        help=(
            "Curated Hudi table URI. Defaults to "
            "s3a://<bucket>/curated/weather."
        ),
    )
    parser.add_argument(
        "--pg-host",
        default="localhost",
        help="PostgreSQL host. Default: localhost",
    )
    parser.add_argument(
        "--pg-port",
        type=int,
        default=5432,
        help="PostgreSQL port. Default: 5432",
    )
    parser.add_argument(
        "--pg-database",
        default="weather_db",
        help="PostgreSQL database. Default: weather_db",
    )
    parser.add_argument(
        "--pg-user",
        default="batch",
        help="PostgreSQL username. Default: batch",
    )
    args = parser.parse_args()

    if not args.endpoint_url.startswith(("http://", "https://")):
        parser.error("--endpoint-url must start with http:// or https://")
    args.password = os.environ.get("WEATHER_DB_PASSWORD")
    if not args.password:
        parser.error(
            "Set the WEATHER_DB_PASSWORD environment variable before running."
        )
    args.source_path = (
        args.source_path or f"s3a://{args.bucket}/curated/weather"
    )
    return args


def create_spark_session(args):
    packages = ",".join(
        [
            (
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:"
                f"{HUDI_VERSION}"
            ),
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            (
                "org.postgresql:postgresql:"
                f"{POSTGRESQL_JDBC_VERSION}"
            ),
        ]
    )

    return (
        SparkSession.builder.appName("cityweather-hudi-to-postgresql")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", args.endpoint_url)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )


def jdbc_url(args):
    return (
        f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/"
        f"{args.pg_database}"
    )


def open_jdbc_connection(spark, args):
    jvm = spark._jvm
    driver_class = (
        jvm.java.lang.Thread.currentThread()
        .getContextClassLoader()
        .loadClass("org.postgresql.Driver")
    )
    driver = driver_class.newInstance()
    properties = jvm.java.util.Properties()
    properties.setProperty("user", args.pg_user)
    properties.setProperty("password", args.password)
    return driver.connect(jdbc_url(args), properties)


def validate_target_table(spark, args):
    connection = open_jdbc_connection(spark, args)
    statement = None
    try:
        statement = connection.createStatement()
        result = statement.executeQuery(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'weather' AND table_name = 'weather_data'"
        )
        try:
            existing_columns = set()
            while result.next():
                existing_columns.add(result.getString(1))
        finally:
            result.close()

        missing_columns = sorted(set(TARGET_COLUMNS) - existing_columns)
        if missing_columns:
            raise RuntimeError(
                "PostgreSQL weather.weather_data is missing columns: "
                f"{', '.join(missing_columns)}. Run the one-time table setup "
                "SQL documented in README.md as the table owner."
            )

        unique_key = statement.executeQuery(
            "SELECT EXISTS ("
            "SELECT 1 FROM pg_index i "
            "JOIN pg_class idx ON idx.oid = i.indexrelid "
            "JOIN pg_class tbl ON tbl.oid = i.indrelid "
            "JOIN pg_namespace ns ON ns.oid = tbl.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = tbl.oid "
            "AND a.attnum = i.indkey[0] "
            "WHERE ns.nspname = 'weather' "
            "AND tbl.relname = 'weather_data' "
            "AND idx.relname = 'weather_data_record_key_uidx' "
            "AND i.indisunique AND i.indnkeyatts = 1 "
            "AND a.attname = 'record_key')"
        )
        try:
            if not unique_key.next() or not unique_key.getBoolean(1):
                raise RuntimeError(
                    "PostgreSQL weather.weather_data needs a unique index "
                    "on record_key. Run the one-time table setup SQL "
                    "documented in README.md as the table owner."
                )
        finally:
            unique_key.close()
    finally:
        if statement is not None:
            statement.close()
        connection.close()


def upsert_curated_records(spark, args, records):
    connection = open_jdbc_connection(spark, args)
    statement = None
    try:
        connection.setAutoCommit(False)
        insert_columns = ", ".join(f'"{name}"' for name in TARGET_COLUMNS)
        placeholders = ", ".join("?" for _ in TARGET_COLUMNS)
        updates = ", ".join(
            f'"{name}" = EXCLUDED."{name}"'
            for name in TARGET_COLUMNS
            if name != "record_key"
        )
        statement = connection.prepareStatement(
            f'INSERT INTO "{TARGET_SCHEMA}"."{TARGET_TABLE}" '
            f"({insert_columns}) VALUES ({placeholders}) "
            'ON CONFLICT ("record_key") DO UPDATE SET '
            f"{updates}"
        )
        java_sql = spark._jvm.java.sql
        sql_types = java_sql.Types
        type_by_column = {
            "record_key": sql_types.VARCHAR,
            "kafka_topic": sql_types.VARCHAR,
            "city": sql_types.VARCHAR,
            "region": sql_types.VARCHAR,
            "country": sql_types.VARCHAR,
            "latitude": sql_types.DOUBLE,
            "longitude": sql_types.DOUBLE,
            "weather_date": sql_types.VARCHAR,
            "weather_time": sql_types.VARCHAR,
            "timezone": sql_types.VARCHAR,
            "fetched_at_utc": sql_types.VARCHAR,
            "temperature_c": sql_types.DOUBLE,
            "feels_like_c": sql_types.DOUBLE,
            "humidity_percent": sql_types.INTEGER,
            "precipitation_mm": sql_types.DOUBLE,
            "wind_speed_kmh": sql_types.DOUBLE,
            "load_date": sql_types.DATE,
        }

        pending = 0
        for row in records.toLocalIterator():
            for index, name in enumerate(TARGET_COLUMNS, start=1):
                value = row[name]
                if value is None:
                    statement.setNull(index, type_by_column[name])
                elif name == "load_date":
                    statement.setDate(
                        index,
                        java_sql.Date.valueOf(str(value)),
                    )
                elif type_by_column[name] == sql_types.VARCHAR:
                    statement.setString(index, str(value))
                elif type_by_column[name] == sql_types.INTEGER:
                    statement.setInt(index, int(value))
                else:
                    statement.setDouble(index, float(value))
            statement.addBatch()
            pending += 1
            if pending == 500:
                statement.executeBatch()
                pending = 0

        if pending:
            statement.executeBatch()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        if statement is not None:
            statement.close()
        connection.close()


def load_curated_data(spark, args):
    records = spark.read.format("hudi").load(args.source_path).select(
        *TARGET_COLUMNS
    )

    if records.filter(records.record_key.isNull()).limit(1).count():
        raise ValueError(
            "Curated Hudi data contains a null record_key; "
            "PostgreSQL upsert requires a record key."
        )

    record_count = records.count()
    if record_count == 0:
        print("Curated Hudi table is empty; nothing to load.", flush=True)
        return

    validate_target_table(spark, args)
    upsert_curated_records(spark, args, records)
    print(
        f"Upserted {record_count} curated weather records into "
        f"{TARGET_SCHEMA}.{TARGET_TABLE} in {args.pg_database}.",
        flush=True,
    )


def main():
    args = parse_arguments()
    spark = None

    try:
        spark = create_spark_session(args)
        spark.sparkContext.setLogLevel("WARN")
        print(f"Curated Hudi source: {args.source_path}", flush=True)
        print(
            f"PostgreSQL target: {args.pg_host}:{args.pg_port}/"
            f"{args.pg_database}.{TARGET_SCHEMA}.{TARGET_TABLE}",
            flush=True,
        )
        load_curated_data(spark, args)
        return 0
    except (
        Py4JError,
        Py4JJavaError,
        PySparkException,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(
            f"\nHudi-to-PostgreSQL load failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
