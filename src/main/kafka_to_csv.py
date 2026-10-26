import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


# Script location: kafksapi/src/main/kafka_to_csv.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "input"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Read weather messages through Docker's Kafka console "
            "consumer and save each message as a raw CSV file."
        )
    )

    parser.add_argument(
        "--container",
        default="kafka-api",
        help="Kafka Docker container name. Default: kafka-api",
    )

    parser.add_argument(
        "--topic",
        default="cityweather",
        help="Kafka topic. Default: cityweather",
    )

    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Broker address INSIDE the container.",
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

    args = parser.parse_args()

    if args.max_messages < 0:
        parser.error("--max-messages must be zero or greater")

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


def stop_consumer(process):
    if process is None:
        return

    if process.poll() is None:
        process.terminate()

        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    if process.stdout is not None:
        process.stdout.close()


def main():
    args = parse_arguments()
    output_dir = args.output_dir.resolve()

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "_"
        + uuid4().hex[:8]
    )

    process = None
    saved_count = 0

    # Do not use -it: Python reads stdout through a pipe.
    command = [
        "docker",
        "exec",
        args.container,
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        args.bootstrap_server,
        "--topic",
        args.topic,
        "--from-beginning",
    ]

    if args.max_messages > 0:
        command.extend([
            "--max-messages",
            str(args.max_messages),
        ])

    try:
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Container: {args.container}", flush=True)
        print(f"Topic: {args.topic}", flush=True)
        print(f"Output directory: {output_dir}", flush=True)
        print(
            "Waiting for weather messages. Press Ctrl+C to stop.",
            flush=True,
        )

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            # Keep Docker/Kafka diagnostics separate from message data.
            stderr=None,
            text=True,
            encoding="utf-8",
            errors="strict",
            bufsize=1,
        )

        for line in process.stdout:
            raw_message = line.rstrip("\r\n")

            if not raw_message.strip():
                continue

            record_number = saved_count + 1

            file_path = save_csv(
                raw_message=raw_message,
                record_number=record_number,
                run_id=run_id,
                topic=args.topic,
                output_dir=output_dir,
            )

            saved_count += 1

            print(
                f"Saved record {record_number}: {file_path.name}",
                flush=True,
            )

        return_code = process.wait()

        if return_code != 0:
            print(
                f"Docker/Kafka consumer exited with code {return_code}.",
                file=sys.stderr,
                flush=True,
            )
            return 1

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

    except json.JSONDecodeError as exc:
        print(
            f"\nReceived a message that is not valid JSON: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    except (OSError, UnicodeError, ValueError) as exc:
        print(
            f"\nConsumer or file error: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        stop_consumer(process)


if __name__ == "__main__":
    sys.exit(main())
