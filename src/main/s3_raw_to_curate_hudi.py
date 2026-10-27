import argparse
import os
import sys

from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, date_format, to_timestamp


SPARK_VERSION = "3.5.9"
HUDI_VERSION = "1.0.2"
HADOOP_AWS_VERSION = "3.3.4"
HUDI_TABLE_NAME = "cityweather_curated"
DEFAULT_BUCKET = "s3-kadka-api-raw"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Transform the raw weather Hudi table into a curated table."
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
        help="Raw Hudi table URI. Defaults to s3a://<bucket>/raw/parquet.",
    )
    parser.add_argument(
        "--target-path",
        default=None,
        help="Curated Hudi table URI. Defaults to s3a://<bucket>/curated/weather.",
    )
    args = parser.parse_args()

    if not args.endpoint_url.startswith(("http://", "https://")):
        parser.error("--endpoint-url must start with http:// or https://")

    args.source_path = args.source_path or f"s3a://{args.bucket}/raw/parquet"
    args.target_path = (
        args.target_path or f"s3a://{args.bucket}/curated/weather"
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
        ]
    )

    return (
        SparkSession.builder.appName("cityweather-raw-to-curated")
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


def curate_weather(spark, source_path, target_path):
    raw = spark.read.format("hudi").load(source_path)
    parsed_weather_time = to_timestamp(col("weather_time"))

    curated = raw.select(
        "record_key",
        "kafka_topic",
        "city",
        "region",
        "country",
        "latitude",
        "longitude",
        date_format(parsed_weather_time, "yyyy-MM-dd").alias("weather_date"),
        date_format(parsed_weather_time, "HH:mm:ss").alias("weather_time"),
        "timezone",
        "fetched_at_utc",
        "temperature_c",
        "feels_like_c",
        "humidity_percent",
        "precipitation_mm",
        "wind_speed_kmh",
        current_date().alias("load_date"),
    )

    if not curated.filter(
        col("weather_date").isNull() | col("weather_time").isNull()
    ).isEmpty():
        raise ValueError(
            "Raw Hudi table contains an invalid weather_time value; "
            "curation was not written."
        )

    curated.show(n=100, truncate=False)

    hudi_options = {
        "hoodie.table.name": HUDI_TABLE_NAME,
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "record_key",
        "hoodie.datasource.write.precombine.field": "fetched_at_utc",
        "hoodie.datasource.write.partitionpath.field": "weather_date",
        "hoodie.datasource.write.keygenerator.class": (
            "org.apache.hudi.keygen.SimpleKeyGenerator"
        ),
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    (
        curated.write.format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save(target_path)
    )
    print(
        f"Curated {curated.count()} weather records into {target_path}",
        flush=True,
    )


def main():
    args = parse_arguments()
    spark = None

    try:
        spark = create_spark_session(args)
        spark.sparkContext.setLogLevel("WARN")
        print(f"Raw Hudi table: {args.source_path}", flush=True)
        print(f"Curated Hudi table: {args.target_path}", flush=True)
        curate_weather(spark, args.source_path, args.target_path)
        return 0
    except (
        Py4JJavaError,
        PySparkException,
        OSError,
        RuntimeError,
        ValueError,
    ) as exc:
        print(
            f"\nRaw-to-curated Hudi job failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
