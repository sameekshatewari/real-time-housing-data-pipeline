\set ON_ERROR_STOP on

-- 1. Create users.
CREATE USER admin  WITH PASSWORD 'admin123';
CREATE USER reader WITH PASSWORD 'reader123';
CREATE USER batch  WITH PASSWORD 'batch123';

-- 2. Create the database.
CREATE DATABASE weather_db OWNER admin;

\connect weather_db

-- 3. Create the schema and table as admin.
SET ROLE admin;

CREATE SCHEMA weather AUTHORIZATION admin;

CREATE TABLE weather.weather_data (
    country            TEXT,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    weather_date       TEXT,
    weather_time       TEXT,
    timezone           TEXT,
    fetched_at_utc     TEXT,
    temperature_c      DOUBLE PRECISION,
    feels_like_c       DOUBLE PRECISION,
    humidity_percent   INTEGER,
    precipitation_mm   DOUBLE PRECISION,
    wind_speed_kmh     DOUBLE PRECISION,
    load_date          DATE NOT NULL
);

CREATE INDEX idx_weather_data_load_date
    ON weather.weather_data (load_date);

-- 4. Restrict general access and allow these users to connect.
REVOKE ALL ON DATABASE weather_db FROM PUBLIC;
GRANT CONNECT ON DATABASE weather_db TO reader, batch;

-- 5. Reader: read only.
GRANT USAGE ON SCHEMA weather TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA weather TO reader;

-- 6. Batch: read, insert, update, and delete.
GRANT USAGE ON SCHEMA weather TO batch;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA weather TO batch;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA weather TO batch;

-- 7. Permissions on future tables/sequences created by admin.
ALTER DEFAULT PRIVILEGES IN SCHEMA weather
    GRANT SELECT ON TABLES TO reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA weather
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO batch;

ALTER DEFAULT PRIVILEGES IN SCHEMA weather
    GRANT USAGE ON SEQUENCES TO batch;

RESET ROLE;


select * from  weather.weather_data 

INSERT INTO weather.weather_data (
    country,
    temperature_c,
    load_date
)
VALUES (
    'Permission test',
    25.0,
    CURRENT_DATE
);

SELECT *
FROM weather.weather_data
WHERE country = 'Permission test';

UPDATE weather.weather_data
SET temperature_c = 26.0
WHERE country = 'Permission test';

DELETE FROM weather.weather_data
WHERE country = 'Permission test';