#!/usr/bin/env bash
set -euo pipefail

project_dir="${KAFKSAPI_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
python="${KAFKSAPI_SPARK_PYTHON:-python}"
job="${project_dir}/src/main/kafka_to_s3_raw_parequet.py"

if [[ ! -f "$job" ]]; then
    echo "Spark job not found: $job" >&2
    echo "Mount the project at KAFKSAPI_PROJECT_DIR in the Airflow task runtime." >&2
    exit 2
fi

if ! command -v "$python" >/dev/null 2>&1; then
    echo "Spark Python executable not found: $python" >&2
    exit 2
fi

exec "$python" "$job" "$@"
