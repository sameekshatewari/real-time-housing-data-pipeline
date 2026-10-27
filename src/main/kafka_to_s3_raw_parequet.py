import argparse
import os
import sys
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException
from pyspark.sql.functions import col, concat_ws, from_json, to_date
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SPARK_VERSION = "3.5.9"
HUDI_VERSION = "1.0.2"
HADOOP_AWS_VERSION = "3.3.4"
HUDI_TABLE_NAME = "cityweather_hudi"
DEFAULT_TOPIC = "cityweather"
DEFAULT_BUCKET = "s3-kadka-api-raw"
DEFAULT_CHECKPOINT = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "checkpoints"
    / "cityweather_hudi"
)

WEATHER_SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("region", StringType()),
        StructField("country", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("weather_time", StringType()),
        StructField("timezone", StringType()),
        StructField("fetched_at_utc", StringType()),
        StructField("temperature_c", DoubleType()),
        StructField("feels_like_c", DoubleType()),
        StructField("humidity_percent", IntegerType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("wind_speed_kmh", DoubleType()),
    ]
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Stream Kafka weather JSON into a Hudi table stored as Parquet "
            "on S3."
        )
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Kafka broker. Default: 127.0.0.1:9092",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic. Default: {DEFAULT_TOPIC}",
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
        "--table-path",
        default=None,
        help="Hudi table URI. Defaults to s3a://<bucket>/raw/parquet.",
    )
    parser.add_argument(
        "--checkpoint-location",
        type=Path,
        default=DEFAULT_CHECKPOINT,
        help="Local Spark checkpoint directory.",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default="earliest",
        help="Kafka starting position when creating a new checkpoint.",
    )
    parser.add_argument(
        "--trigger-interval",
        default="10 seconds",
        help='Spark micro-batch interval. Default: "10 seconds".',
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help=(
            "Process Kafka records currently available and exit. "
            "Useful for a finite Airflow task."
        ),
    )
    args = parser.parse_args()

    servers = []
    for server in args.bootstrap_server.split(","):
        address = server.strip()
        host, separator, port = address.rpartition(":")
        if separator and host.lower() == "localhost":
            address = f"127.0.0.1:{port}"
        if not separator or not host or not port:
            parser.error("--bootstrap-server entries must be hostname:port")
        servers.append(address)
    args.bootstrap_server = ",".join(servers)

    if not args.endpoint_url.startswith(("http://", "https://")):
        parser.error("--endpoint-url must start with http:// or https://")

    args.table_path = args.table_path or f"s3a://{args.bucket}/raw/parquet"
    return args


def create_spark_session(args):
    packages = ",".join(
        [
            (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:"
                f"{SPARK_VERSION}"
            ),
            (
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:"
                f"{HUDI_VERSION}"
            ),
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
        ]
    )

    return (
        SparkSession.builder.appName("cityweather-kafka-to-hudi")
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
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.s3a.endpoint", args.endpoint_url)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID", "test"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .getOrCreate()
    )


def write_micro_batch(batch_df, batch_id, table_path):
    if batch_df.isEmpty():
        return

    parsed = (
        batch_df.withColumn(
            "weather",
            from_json(col("kafka_message"), WEATHER_SCHEMA),
        )
    )

    invalid = parsed.filter(
        col("weather").isNull()
        | col("weather.city").isNull()
        | col("weather.weather_time").isNull()
    )
    if not invalid.isEmpty():
        raise ValueError(
            f"Kafka micro-batch {batch_id} contains malformed weather JSON "
            "or is missing city/weather_time."
        )

    records = parsed.select(
        concat_ws(
            ":",
            col("topic"),
            col("partition").cast("string"),
            col("offset").cast("string"),
        ).alias("record_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp"),
        col("kafka_message"),
        col("weather.city").alias("city"),
        col("weather.region").alias("region"),
        col("weather.country").alias("country"),
        col("weather.latitude").alias("latitude"),
        col("weather.longitude").alias("longitude"),
        col("weather.weather_time").alias("weather_time"),
        col("weather.timezone").alias("timezone"),
        col("weather.fetched_at_utc").alias("fetched_at_utc"),
        col("weather.temperature_c").alias("temperature_c"),
        col("weather.feels_like_c").alias("feels_like_c"),
        col("weather.humidity_percent").alias("humidity_percent"),
        col("weather.precipitation_mm").alias("precipitation_mm"),
        col("weather.wind_speed_kmh").alias("wind_speed_kmh"),
        to_date(col("weather.weather_time")).alias("weather_date"),
    )

    hudi_options = {
        "hoodie.table.name": HUDI_TABLE_NAME,
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "record_key",
        "hoodie.datasource.write.precombine.field": "kafka_timestamp",
        "hoodie.datasource.write.partitionpath.field": "weather_date",
        "hoodie.datasource.write.keygenerator.class": (
            "org.apache.hudi.keygen.SimpleKeyGenerator"
        ),
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    (
        records.write.format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save(table_path)
    )

    print(
        f"Committed Kafka micro-batch {batch_id} to {table_path}",
        flush=True,
    )


def main():
    args = parse_arguments()
    spark = None
    query = None

    try:
        args.checkpoint_location.mkdir(parents=True, exist_ok=True)
        checkpoint_uri = args.checkpoint_location.resolve().as_uri()

        spark = create_spark_session(args)
        spark.sparkContext.setLogLevel("WARN")

        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_server)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "true")
            .load()
        )

        raw_weather = kafka_stream.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("kafka_message"),
        )

        print(f"Kafka topic: {args.topic}", flush=True)
        print(f"Hudi table: {args.table_path}", flush=True)
        print(f"Spark checkpoint: {checkpoint_uri}", flush=True)
        print(
            f"Starting Kafka stream from {args.starting_offsets} "
            "for a new checkpoint. Press Ctrl+C to stop.",
            flush=True,
        )

        writer = (
            raw_weather.writeStream.foreachBatch(
                lambda batch_df, batch_id: write_micro_batch(
                    batch_df,
                    batch_id,
                    args.table_path,
                )
            )
            .option("checkpointLocation", checkpoint_uri)
        )
        if args.available_now:
            query = writer.trigger(availableNow=True).start()
        else:
            query = writer.trigger(
                processingTime=args.trigger_interval
            ).start()

        query.awaitTermination()
        return 0

    except KeyboardInterrupt:
        print("\nStopping Spark streaming query...", flush=True)
        if query is not None:
            query.stop()
        return 130

    except (
        Py4JJavaError,
        PySparkException,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(
            f"\nKafka-to-Hudi streaming failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
