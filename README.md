# Kafka weather ingestion

Fetch current weather for a city and publish a JSON record to the Kafka topic
`cityweather`.

## Kafka container

The `kafka-api` container publishes Kafka only to this Windows machine and
advertises the same IPv4 address to Kafka clients. Its data is stored in the
Docker volume `kafka-api-data`.

To start the existing container after a restart:

```powershell
docker start kafka-api
```

To create the container if it does not exist:

```powershell
docker volume create kafka-api-data
docker run -d --name kafka-api --restart unless-stopped `
  -p 127.0.0.1:9092:9092 `
  -e "CLUSTER_ID=5L6g3nShT-eMCtK--X86sw" `
  -e "KAFKA_NODE_ID=1" `
  -e "KAFKA_PROCESS_ROLES=broker,controller" `
  -e "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093" `
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092" `
  -e "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER" `
  -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093" `
  -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" `
  -e "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT" `
  -e "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1" `
  -e "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1" `
  -e "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1" `
  -e "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0" `
  -e "KAFKA_NUM_PARTITIONS=1" `
  -v kafka-api-data:/tmp/kraft-combined-logs `
  apache/kafka:latest
```

## Test Kafka, producer, and consumer

From the project root, run the end-to-end test. It checks the broker, creates
`cityweather` with one partition if needed, runs the weather ingestion program,
and consumes the new weather record:

```powershell
.\.venv\Scripts\python.exe .\test\test.py Dallas
```

## Run ingestion

Publish one record:

```powershell
.\.venv\Scripts\python.exe .\src\main\kafka_ingestion.py Dallas
```

Publish repeatedly every 60 seconds:

```powershell
.\.venv\Scripts\python.exe .\src\main\kafka_ingestion.py Dallas --interval 60
```

Use `Ctrl+C` to stop continuous ingestion. The bootstrap server defaults to
`127.0.0.1:9092`; override it with `--bootstrap-server` when needed.

## Copy Kafka weather records to LocalStack S3

Install the S3 client dependency into the project virtual environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

The `kafka_to_s3_raw_csv.py` consumer reads JSON weather messages from
`cityweather` and stores them as rows in
`s3://s3-kadka-api-raw/raw/csv/weather.csv`. The CSV keeps the original JSON in
`kafka_message` and records the Kafka topic, partition, and offset. S3 objects
are not appendable: for each batch, the consumer reads the existing CSV,
adds new rows, then replaces the object with the complete expanded CSV. Kafka
offsets are committed only after a successful upload; replayed offsets are
skipped.

Ensure the bucket exists in LocalStack, then start the consumer:

```powershell
aws --endpoint-url=http://localhost:4566 s3 mb s3://s3-kadka-api-raw
.\.venv\Scripts\python.exe .\src\main\kafka_to_s3_raw_csv.py
```

The consumer runs continuously; press `Ctrl+C` to stop. It defaults to Kafka
at `127.0.0.1:9092` and LocalStack at `http://localhost:4566`. Use
`--max-messages 1` for a one-record test, or set `--batch-size` to upload
multiple records per S3 request.

Read the resulting object from PowerShell:

```powershell
aws --endpoint-url=http://localhost:4566 s3 cp s3://s3-kadka-api-raw/raw/csv/weather.csv -
```

Remove the raw object if you want to clear its contents:

```powershell
aws --endpoint-url=http://localhost:4566 s3 rm s3://s3-kadka-api-raw/raw/csv/weather.csv
```

The S3 raw writer uses a dedicated Kafka consumer group
(`cityweather-s3-raw` by default), so it does not compete with other consumers.
Use the same group ID on restarts to resume from its committed offsets. If you
delete `raw/csv/weather.csv` and want to rebuild it from all retained Kafka
messages, use a new group ID:

```powershell
.\.venv\Scripts\python.exe .\src\main\kafka_to_s3_raw_csv.py --group-id cityweather-s3-raw-rebuild
```

## Stream Kafka weather records to a Hudi table

The Spark Structured Streaming job consumes `cityweather`, parses the weather
JSON, and writes a Hudi Copy-on-Write table (Parquet base files) to
`s3a://s3-kadka-api-raw/raw/parquet`. Every Kafka topic/partition/offset has a
stable Hudi record key, so replaying a batch is an upsert rather than a
duplicate. Weather records are partitioned by `weather_date`.
The auxiliary Hudi metadata table is disabled for compatibility with the
Windows/S3A LocalStack setup.

Install the project dependencies and start the stream:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
.\.venv\Scripts\python.exe .\src\main\kafka_to_s3_raw_parequet.py
```

The job uses Spark 3.5.9, Hudi 1.0.2, and Hadoop S3A 3.3.4 dependencies.
Spark downloads the Kafka, Hudi, and S3A Java packages when it first starts,
so the first launch needs Maven Central access. It uses the local Java runtime,
connects to Kafka at `127.0.0.1:9092`, and connects to LocalStack S3 at
`http://localhost:4566`. The Spark checkpoint is stored locally at
`data/checkpoints/cityweather_hudi`; keep it to resume the same stream.
Press `Ctrl+C` to stop.

To start an interactive Spark session with the same Hudi, Kafka, and S3A
dependencies and LocalStack settings as the streaming job, run this from the
project root:

```powershell
.\.venv\Scripts\python.exe -i -c "import sys; sys.path.insert(0, r'.\src\main'); import kafka_to_s3_raw_parequet as hudi_job; args = hudi_job.parse_arguments(); spark = hudi_job.create_spark_session(args); spark.sparkContext.setLogLevel('WARN')"
```

At the Python prompt, read and query the Hudi table:

```python
table = spark.read.format("hudi").load("s3a://s3-kadka-api-raw/raw/parquet"); 
table.createOrReplaceTempView("cityweather"); 
spark.sql("SELECT city, weather_time, temperature_c FROM cityweather ORDER BY weather_time DESC").show(truncate=False);
```

When finished, stop Spark and exit the Python prompt:

```python
spark.stop()
exit()
```

## Curate raw weather into a Hudi table

Run the batch curation job from the project root after the raw Hudi stream has
written data:

```powershell
.\.venv\Scripts\python.exe .\src\main\s3_raw_to_curate_hudi.py
```

The job reads `s3a://s3-kadka-api-raw/raw/parquet` and upserts the selected
weather columns to the curated Hudi table at
`s3a://s3-kadka-api-raw/curated/weather`. It splits the source
`weather_time` timestamp into `weather_date` and a time-only `weather_time`,
and adds `load_date` (UTC) as an audit column. The curated table is
Copy-on-Write Parquet partitioned by `weather_date`; rerunning the job updates
existing records by `record_key` instead of duplicating them.

To query the curated table in the interactive Spark session described above:

```python
curated = spark.read.format("hudi").load("s3a://s3-kadka-api-raw/curated/weather")
curated.select("record_key", "city", "weather_date", "weather_time", "load_date", "temperature_c").show(100, truncate=False)
```

## Load curated Hudi data into PostgreSQL

The `s3_hudi_to_dbload.py` batch job reads the curated Hudi table and upserts
its records into `weather.weather_data` in the local PostgreSQL database
`weather_db`. It uses `record_key` for idempotent upserts. Since the existing
table is owned by `postgres`, first run this one-time setup SQL in VS Code
while connected as the table owner:

```sql
ALTER TABLE weather.weather_data
    ADD COLUMN IF NOT EXISTS record_key TEXT,
    ADD COLUMN IF NOT EXISTS kafka_topic TEXT,
    ADD COLUMN IF NOT EXISTS city TEXT,
    ADD COLUMN IF NOT EXISTS region TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS weather_data_record_key_uidx
    ON weather.weather_data (record_key);
```

The loader writes directly to `weather.weather_data` in batches and uses the
unique `record_key` index for upserts; it does not create a staging schema.
Existing weather rows are not deleted.

Set the database password in the current PowerShell session and run the job
from the project root:

```powershell
$env:WEATHER_DB_PASSWORD = "batch123"
.\.venv\Scripts\python.exe .\src\main\s3_hudi_to_dbload.py
```

The defaults are PostgreSQL at `localhost:5432`, database `weather_db`, and
user `batch`. The password is read from `WEATHER_DB_PASSWORD`, not stored in
the script. The Spark job fetches the PostgreSQL JDBC driver along with the
Hudi and S3A packages on first launch.
