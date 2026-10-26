import argparse
import json
import socket
import sys
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from weather_api import get_weather


TOPIC_NAME = "cityweather"


class KafkaEndpointUnavailable(ConnectionError):
    """Raised when no configured Kafka bootstrap endpoint accepts connections."""


def normalize_bootstrap_servers(bootstrap_servers):
    """Use IPv4 for localhost to match the Docker-published broker listener."""
    normalized_servers = []

    for server in bootstrap_servers.split(","):
        address = server.strip()
        host, separator, port = address.rpartition(":")

        if separator and host.lower() == "localhost":
            address = f"127.0.0.1:{port}"

        normalized_servers.append(address)

    return ",".join(normalized_servers)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Fetch city weather and publish it to Kafka."
    )

    parser.add_argument(
        "city",
        help='City name, for example Dallas or "New York".',
    )

    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Kafka broker address. Default: 127.0.0.1:9092",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Seconds between records. Use 0 to publish once and exit.",
    )

    args = parser.parse_args()

    if args.interval < 0:
        parser.error("--interval must be zero or greater")

    return args


def check_broker_reachable(bootstrap_servers):
    """Fail quickly with a clear message if a bootstrap address is not reachable."""
    servers = (
        bootstrap_servers.split(",")
        if isinstance(bootstrap_servers, str)
        else bootstrap_servers
    )
    errors = []

    for server in servers:
        address = server.strip()
        if address.startswith("["):
            closing_bracket = address.find("]")
            if closing_bracket < 0 or address[closing_bracket + 1 : closing_bracket + 2] != ":":
                errors.append(f"{address}: expected [IPv6]:port")
                continue
            host = address[1:closing_bracket]
            port_text = address[closing_bracket + 2 :]
        else:
            host, separator, port_text = address.rpartition(":")
            if not separator or not host:
                errors.append(f"{address}: expected hostname:port")
                continue

        try:
            port = int(port_text)
            if not 1 <= port <= 65535:
                raise ValueError
        except ValueError:
            errors.append(f"{address}: port must be between 1 and 65535")
            continue

        try:
            with socket.create_connection((host, port), timeout=3):
                return
        except OSError as exc:
            errors.append(f"{address}: {exc}")

    details = "; ".join(errors)
    raise KafkaEndpointUnavailable(
        f"Cannot connect to Kafka bootstrap server(s): {details}. "
        "Publish the broker port to Windows and configure an advertised "
        "listener reachable at the same address."
    )


def create_producer(bootstrap_server):
    bootstrap_server = normalize_bootstrap_servers(bootstrap_server)

    print(
        f"Connecting to Kafka at {bootstrap_server}...",
        flush=True,
    )

    check_broker_reachable(bootstrap_server)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        client_id="cityweather-producer",
        acks="all",
        max_block_ms=15000,
        bootstrap_timeout_ms=10000,
    )

    print("Kafka producer initialized.", flush=True)
    return producer


def publish_weather(producer, city):
    print(f"Fetching weather for {city}...", flush=True)

    weather = get_weather(city)

    print(
        f"Weather received for {weather['city']}, "
        f"{weather.get('region', '')}, "
        f"{weather.get('country', '')}.",
        flush=True,
    )

    message_key = (
        f"{weather['city']}|"
        f"{weather.get('region', '')}|"
        f"{weather.get('country', '')}"
    )

    key_bytes = message_key.encode("utf-8")

    value_bytes = json.dumps(
        weather,
        ensure_ascii=False,
    ).encode("utf-8")

    print(
        f"Sending weather to topic '{TOPIC_NAME}'...",
        flush=True,
    )

    future = producer.send(
        TOPIC_NAME,
        key=key_bytes,
        value=value_bytes,
    )

    metadata = future.get(timeout=30)

    print(
        f"Published successfully: "
        f"topic={metadata.topic}, "
        f"partition={metadata.partition}, "
        f"offset={metadata.offset}",
        flush=True,
    )

    print(
        json.dumps(weather, indent=2, ensure_ascii=False),
        flush=True,
    )


def main():
    args = parse_arguments()
    producer = None

    try:
        producer = create_producer(args.bootstrap_server)

        while True:
            publish_weather(producer, args.city)

            if args.interval == 0:
                print("One record published. Finished.", flush=True)
                break

            print(
                f"Waiting {args.interval} seconds. "
                "Press Ctrl+C to stop.",
                flush=True,
            )

            time.sleep(args.interval)

        return 0

    except KeyboardInterrupt:
        print("\nIngestion stopped by user.", flush=True)
        return 130

    except KafkaEndpointUnavailable as exc:
        print(f"\nKafka endpoint unavailable: {exc}", file=sys.stderr, flush=True)
        return 1

    except KafkaError as exc:
        print(
            f"\nKafka error: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )

        print(
            "Check the published Docker port, Kafka advertised "
            "listeners, and topic 'cityweather'.",
            file=sys.stderr,
            flush=True,
        )

        return 1

    except requests.exceptions.RequestException as exc:
        print(
            f"\nWeather API request failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    except (ValueError, KeyError) as exc:
        print(
            f"\nData or configuration error: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    finally:
        if producer is not None:
            print("Closing Kafka producer...", flush=True)

            try:
                producer.close(timeout=5)
            except Exception as exc:
                print(
                    f"Warning while closing producer: {exc}",
                    file=sys.stderr,
                    flush=True,
                )


if __name__ == "__main__":
    sys.exit(main())
