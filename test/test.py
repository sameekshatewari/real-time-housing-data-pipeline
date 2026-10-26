import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError


PROJECT_DIR = Path(__file__).resolve().parents[1]
INGESTION_PROGRAM = PROJECT_DIR / "src" / "main" / "kafka_ingestion.py"
TOPIC_NAME = "cityweather"


def ensure_topic(admin):
    if TOPIC_NAME not in admin.list_topics():
        print(f"Creating topic '{TOPIC_NAME}' with one partition...", flush=True)
        admin.create_topics(
            [NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)],
            wait_for_metadata=True,
        )

    topic = admin.describe_topics([TOPIC_NAME])[0]
    partitions = topic["partitions"]
    if len(partitions) != 1:
        raise RuntimeError(
            f"Expected topic '{TOPIC_NAME}' to have one partition; "
            f"found {len(partitions)}."
        )

    partition = partitions[0]["partition_index"]
    print(
        f"Kafka is reachable; topic '{TOPIC_NAME}' has one partition.",
        flush=True,
    )
    return partition


def main():
    parser = argparse.ArgumentParser(
        description="Check Kafka and verify weather ingestion with a consumer."
    )
    parser.add_argument("city", nargs="?", default="Dallas")
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Kafka broker address. Default: 127.0.0.1:9092",
    )
    args = parser.parse_args()
    args.bootstrap_server = ",".join(
        f"127.0.0.1:{server.rpartition(':')[2]}"
        if server.rpartition(":")[0].lower() == "localhost"
        else server.strip()
        for server in args.bootstrap_server.split(",")
    )

    admin = None
    consumer = None

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=args.bootstrap_server,
            client_id="cityweather-smoke-test",
            request_timeout_ms=10000,
        )
        partition = ensure_topic(admin)

        topic_partition = TopicPartition(TOPIC_NAME, partition)
        consumer = KafkaConsumer(
            bootstrap_servers=args.bootstrap_server,
            enable_auto_commit=False,
            group_id=None,
        )
        consumer.assign([topic_partition])
        consumer.poll(timeout_ms=1000)
        start_offset = consumer.end_offsets([topic_partition])[topic_partition]
        consumer.seek(topic_partition, start_offset)

        result = subprocess.run(
            [
                sys.executable,
                str(INGESTION_PROGRAM),
                args.city,
                "--bootstrap-server",
                args.bootstrap_server,
            ],
            cwd=PROJECT_DIR,
            check=False,
        )
        if result.returncode:
            return result.returncode

        print(f"Waiting for a new record on '{TOPIC_NAME}'...", flush=True)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    weather = json.loads(record.value.decode("utf-8"))
                    if "city" in weather and "temperature_c" in weather:
                        print(
                            f"Consumer verified a weather record for "
                            f"{weather['city']} at offset {record.offset}.",
                            flush=True,
                        )
                        return 0

        raise TimeoutError(
            f"No weather record was consumed from '{TOPIC_NAME}' "
            "within 15 seconds."
        )

    except (
        KafkaError,
        OSError,
        TimeoutError,
        RuntimeError,
        KeyError,
        ValueError,
    ) as exc:
        print(f"Kafka smoke test failed: {exc}", file=sys.stderr, flush=True)
        return 1

    finally:
        if consumer is not None:
            consumer.close()
        if admin is not None:
            admin.close()


if __name__ == "__main__":
    sys.exit(main())
