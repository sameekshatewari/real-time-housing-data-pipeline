import argparse
import csv
import io
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from kafka import KafkaConsumer
from kafka.errors import KafkaError


TOPIC_NAME = "cityweather"
BUCKET_NAME = "s3-kadka-api-raw"
OBJECT_KEY = "raw/csv/weather.csv"
CSV_FIELDS = [
    "record_number",
    "topic",
    "partition",
    "offset",
    "saved_at_utc",
    "kafka_message",
]


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Consume Kafka weather JSON and store raw records in S3."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Kafka broker. Default: 127.0.0.1:9092",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC_NAME,
        help=f"Kafka topic. Default: {TOPIC_NAME}",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        help="S3 endpoint. Default: http://localhost:4566",
    )
    parser.add_argument(
        "--bucket",
        default=BUCKET_NAME,
        help=f"S3 bucket. Default: {BUCKET_NAME}",
    )
    parser.add_argument(
        "--key",
        default=OBJECT_KEY,
        help=f"S3 object key. Default: {OBJECT_KEY}",
    )
    parser.add_argument(
        "--group-id",
        default="cityweather-s3-raw",
        help="Kafka consumer group. Default: cityweather-s3-raw",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Maximum Kafka records per S3 upload. Default: 1",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after consuming N records. Use 0 to run continuously.",
    )

    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be greater than zero")
    if args.max_messages < 0:
        parser.error("--max-messages must be zero or greater")

    normalized_servers = []
    for server in args.bootstrap_server.split(","):
        address = server.strip()
        host, separator, port = address.rpartition(":")
        if separator and host.lower() == "localhost":
            address = f"127.0.0.1:{port}"
        normalized_servers.append(address)
    args.bootstrap_server = ",".join(normalized_servers)
    if not args.bootstrap_server:
        parser.error("--bootstrap-server must contain hostname:port")

    return args


def create_s3_client(endpoint_url):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=os.getenv(
            "AWS_DEFAULT_REGION",
            os.getenv("AWS_REGION", "us-east-1"),
        ),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(s3={"addressing_style": "path"}),
    )


def load_existing_rows(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return []
        raise

    content = response["Body"].read().decode("utf-8")
    if not content:
        return []

    reader = csv.DictReader(io.StringIO(content, newline=""))
    if reader.fieldnames != CSV_FIELDS:
        raise ValueError(
            f"s3://{bucket}/{key} has an unexpected CSV header; "
            "refusing to overwrite it."
        )

    rows = list(reader)
    for row in rows:
        try:
            int(row["partition"])
            int(row["offset"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"s3://{bucket}/{key} contains an invalid Kafka position."
            ) from exc
    return rows


def serialize_rows(rows):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def upload_rows(s3_client, bucket, key, rows):
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=serialize_rows(rows),
        ContentType="text/csv; charset=utf-8",
    )


def make_record(topic, message, record_number):
    raw_message = message.value.decode("utf-8")
    json.loads(raw_message)

    return {
        "record_number": record_number,
        "topic": topic,
        "partition": message.partition,
        "offset": message.offset,
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "kafka_message": raw_message,
    }


def main():
    args = parse_arguments()
    s3_client = None
    consumer = None
    saved_count = 0
    processed_count = 0

    try:
        s3_client = create_s3_client(args.endpoint_url)
        rows = load_existing_rows(s3_client, args.bucket, args.key)
        known_positions = {
            (row["topic"], int(row["partition"]), int(row["offset"]))
            for row in rows
        }

        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap_server,
            group_id=args.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=args.batch_size,
        )

        destination = f"s3://{args.bucket}/{args.key}"
        print(f"Kafka topic: {args.topic}", flush=True)
        print(f"S3 destination: {destination}", flush=True)
        print(f"Existing CSV records: {len(rows)}", flush=True)
        print("Waiting for Kafka records. Press Ctrl+C to stop.", flush=True)

        while True:
            remaining = (
                args.max_messages - processed_count
                if args.max_messages
                else args.batch_size
            )
            if args.max_messages and remaining <= 0:
                break

            messages = consumer.poll(
                timeout_ms=1000,
                max_records=min(args.batch_size, remaining),
            )
            batch = [
                message
                for partition_messages in messages.values()
                for message in partition_messages
            ]
            if not batch:
                continue
            processed_count += len(batch)

            new_rows = []
            for message in batch:
                position = (message.topic, message.partition, message.offset)
                if position in known_positions:
                    continue

                new_rows.append(
                    make_record(
                        topic=message.topic,
                        message=message,
                        record_number=len(rows) + len(new_rows) + 1,
                    )
                )
                known_positions.add(position)

            if new_rows:
                updated_rows = rows + new_rows
                upload_rows(s3_client, args.bucket, args.key, updated_rows)
                rows = updated_rows
                saved_count += len(new_rows)
                print(
                    f"Uploaded {len(new_rows)} record(s); "
                    f"total CSV records={len(rows)} "
                    f"destination={destination}",
                    flush=True,
                )

            # Commit only after S3 succeeds. A replay after an interrupted
            # commit is detected by its topic, partition, and offset in the CSV.
            consumer.commit()

        print(f"Finished. New records uploaded: {saved_count}", flush=True)
        return 0

    except KeyboardInterrupt:
        print(
            f"\nStopped. New records uploaded: {saved_count}",
            flush=True,
        )
        return 130

    except json.JSONDecodeError as exc:
        print(
            f"\nKafka message was not valid JSON: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    except (
        KafkaError,
        BotoCoreError,
        ClientError,
        OSError,
        UnicodeError,
        ValueError,
    ) as exc:
        print(
            f"\nKafka-to-S3 error: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        if consumer is not None:
            consumer.close()
        if s3_client is not None:
            s3_client.close()


if __name__ == "__main__":
    sys.exit(main())
