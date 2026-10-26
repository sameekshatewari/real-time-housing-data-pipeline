import argparse
import json
import sys
from datetime import datetime, timezone

import requests


GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"


def get_json(url, params):
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise ValueError(data.get("reason", "API returned an error"))

    return data


def get_weather(city):
    # Resolve the city name to geographic coordinates.
    location_data = get_json(
        GEOCODING_URL,
        {
            "name": city,
            "count": 1,
            "language": "en",
            "format": "json",
        },
    )

    locations = location_data.get("results", [])

    if not locations:
        raise ValueError(f"No matching city found for: {city}")

    location = locations[0]

    # Request current weather with explicit measurement units.
    weather_data = get_json(
        WEATHER_URL,
        {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "current": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "apparent_temperature,"
                "precipitation,"
                "wind_speed_10m"
            ),
            "temperature_unit": "celsius",
            "wind_speed_unit": "kmh",
            "precipitation_unit": "mm",
            "timezone": "auto",
        },
    )

    current = weather_data.get("current")

    if not current:
        raise ValueError("The API did not return current weather data")

    # A flat record makes later Kafka and CSV integration straightforward.
    return {
        "city": location["name"],
        "region": location.get("admin1", ""),
        "country": location.get("country", ""),
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "weather_time": current["time"],
        "timezone": weather_data["timezone"],
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "temperature_c": current["temperature_2m"],
        "feels_like_c": current["apparent_temperature"],
        "humidity_percent": current["relative_humidity_2m"],
        "precipitation_mm": current["precipitation"],
        "wind_speed_kmh": current["wind_speed_10m"],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Fetch current weather for a city."
    )
    parser.add_argument(
        "city",
        help='City name, for example: "Dallas" or "New York"',
    )
    args = parser.parse_args()

    try:
        weather = get_weather(args.city)
        print(json.dumps(weather, indent=2, ensure_ascii=False))
        return 0

    except requests.exceptions.RequestException as exc:
        print(f"API request failed: {exc}", file=sys.stderr)
        return 1

    except (ValueError, KeyError) as exc:
        print(f"Could not read weather data: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())