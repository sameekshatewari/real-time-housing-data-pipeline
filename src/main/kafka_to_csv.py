import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kafka import KafkaConsumer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "input"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Consume Kafka weather messages and save each as a local CSV file."
        )
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Kafka broker address. Default: 127.0.0.1:9092",
    )
    parser.add_argument(
        "--topic",
        default="cityweather",
        help="Kafka topic. Default: cityweather",
    )
    parser.add_argument(
        "--group-id",
        default="cityweather-local-csv",
        help="Kafka consumer group. Default: cityweather-local-csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Default: project-root/data/input",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N saved messages. Default: 0 runs continuously.",
    )
    parser.add_argument(
        "--idle-timeout-seconds",
        type=int,
        default=0,
        help="Stop after this many seconds without a message; 0 waits forever.",
    )
    args = parser.parse_args()

    if args.max_messages < 0 or args.idle_timeout_seconds < 0:
        parser.error(
            "--max-messages and --idle-timeout-seconds must be zero or greater"
        )

    return args


def save_csv(raw_message, record_number, run_id, topic, output_dir):
    # Validate the weather JSON, but retain its original text.
    json.loads(raw_message)

    filename = (
        f"{topic}_kafka_message_"
        f"record_{record_number:06d}_"
        f"{run_id}.csv"
    )

    final_path = output_dir / filename
    temporary_path = output_dir / f"{filename}.tmp"

    row = {
        "record_number": record_number,
        "topic": topic,
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "kafka_message": raw_message,
    }

    try:
        with temporary_path.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=list(row.keys()),
            )
            writer.writeheader()
            writer.writerow(row)
            file.flush()
            os.fsync(file.fileno())

        temporary_path.replace(final_path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    return final_path


def main():
    args = parse_arguments()
    output_dir = args.output_dir.resolve()

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "_"
        + uuid4().hex[:8]
    )
    saved_count = 0
    consumer = None

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Kafka broker: {args.bootstrap_server}", flush=True)
        print(f"Topic: {args.topic}", flush=True)
        print(f"Output directory: {output_dir}", flush=True)
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap_server,
            group_id=args.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=args.max_messages or 100,
        )
        print(
            f"Consumer group: {args.group_id}. Waiting for weather messages.",
            flush=True,
        )
        last_message_at = time.monotonic()
        while not args.max_messages or saved_count < args.max_messages:
            remaining = (
                args.max_messages - saved_count
                if args.max_messages
                else 100
            )
            records = [
                message
                for partition_records in consumer.poll(
                    timeout_ms=1000,
                    max_records=min(100, remaining),
                ).values()
                for message in partition_records
            ]
            if not records:
                if (
                    args.idle_timeout_seconds
                    and time.monotonic() - last_message_at
                    >= args.idle_timeout_seconds
                ):
                    break
                continue

            for message in records:
                raw_message = message.value.decode("utf-8")
                file_path = save_csv(
                    raw_message=raw_message,
                    record_number=saved_count + 1,
                    run_id=run_id,
                    topic=message.topic,
                    output_dir=output_dir,
                )
                saved_count += 1
                last_message_at = time.monotonic()
                consumer.commit()
                print(
                    f"Saved record {saved_count}: {file_path.name}",
                    flush=True,
                )
                if args.max_messages and saved_count >= args.max_messages:
                    break

        print(
            f"Consumer finished. CSV files saved: {saved_count}",
            flush=True,
        )
        return 0

    except KeyboardInterrupt:
        print(
            f"\nStopped. CSV files saved: {saved_count}",
            flush=True,
        )
        return 130

    except (
        KafkaError,
        OSError,
        UnicodeError,
        ValueError,
    ) as exc:
        print(
            f"\nKafka consumer or local CSV write failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    sys.exit(main())
