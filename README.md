

# Data pipeline summary

This data pipeline ingests data from the Open Brewery DB API, performs transformations, and loads it into a data lake structured with Bronze, Silver, and Gold layers. 
It leverages Apache Airflow for orchestration, ensuring scheduled execution, error handling, and dependency management between the stages. 
Google Cloud Storage is used for data storage, and BigQuery serves as the final destination for analytical queries.

Medallion Architecture: Implements the Bronze, Silver, Gold layered approach for data lake organization.
Incremental Loading: Only processes new or changed data, improving efficiency.
Data Validation: Uses hash comparisons to ensure data integrity.
Monitoring and Alerting: Includes logging to GCS and email alerts for proactive monitoring.
Orchestration: Utilizes Apache Airflow for scheduling, task dependencies, and error handling. 

Data pipeline summary

This pipeline ingests the Open Brewery DB API and materializes a Medallion data lake (Bronze → Silver → Gold) orchestrated by Apache Airflow (Cloud Composer 3). Files are stored in GCS and analytical tables in BigQuery.

End-to-end flow

Bronze (raw snapshot): The DAG paginates the API (per_page=200) and iterates until an empty page is returned—so all pages are fetched, not just page 1. The full snapshot is serialized to a single UTF-8 NDJSON and written to GCS, plus a timestamped archive copy.

Change detection & idempotency: A SHA-256 is computed over the NDJSON bytes. The hash is compared to the previous run’s value stored in gs://us-central1-composer-case-165cfec3-bucket/control/bronze_sha256.txt.

If unchanged, Silver/Gold are skipped automatically (incremental gate).

If changed (or forced), the pipeline proceeds to Silver and Gold.

Silver (clean & typed): Reads Bronze from BigQuery, applies text repairs for known mojibake (e.g., Caf�→Café, Stra�e→Straße, K�rnten→Kärnten, Nieder�sterreich→Niederösterreich, W�rthersee→Wörthersee, Wimitzbr�u→Wimitzbräu), normalizes Unicode (NFC), and casts longitude/latitude to FLOAT64. It derives a deterministic state_partition integer and writes Parquet to GCS, then loads Medallion.silver (range-partitioned by state_partition and clustered by country, city).

Gold (aggregated): Aggregates Silver to counts per country, state, brewery_type, writes Parquet to GCS, and loads Medallion.gold (clustered by country, state, brewery_type).

Storage & control artifacts (GCS)

Bronze NDJSON (current): gs://us-central1-composer-case-165cfec3-bucket/data/bronze/breweries.ndjson

Bronze NDJSON (archive): .../data/bronze/archive/breweries_<timestamp>.ndjson

Silver Parquet: .../data/silver/breweries_transformed.parquet + archive

Gold Parquet: .../data/gold/breweries_aggregated.parquet + archive

Control file (hash): gs://us-central1-composer-case-165cfec3-bucket/control/bronze_sha256.txt

Per-run logs: .../logs/<timestamp>/{bronze|silver|gold}.log

Scheduling & forcing

Schedule: 0 3 * * * (daily at 03:00 UTC, ≈00:00 America/Sao_Paulo). catchup=false.

Force a full run (bypass the hash gate) via:

Airflow Run Config: {"force": true}, or

Airflow Variable: bees_force=true (then trigger normally).

Operational characteristics

Incremental loading: downstream stages run only when data changes, saving cost and time.

Reliability: API calls use timeouts; Airflow handles retries and task-level error handling.

Observability: Each stage writes a concise log to GCS with page/row counts, hash values, and outcomes (“changed”/“no change”).

# For detailed DOCKER deployment, please go to folder "DOCKER"


# Overview

A data pipeline that ingests the Open Brewery DB API and organizes it in a Medallion architecture (Bronze → Silver → Gold) orchestrated by Apache Airflow (Cloud Composer 3 / Airflow 2.x). Raw/derived files live in GCS; analytical tables live in BigQuery.

- **GCP Project:** `case-abinbev-469918`
- **BQ Dataset:** `Medallion`
- **BQ Tables:** `bronze`, `silver`, `gold`
- **Composer bucket:** `us-central1-composer-case-165cfec3-bucket`
- **DAG:** `bees_breweries_daily`
- **Schedule:** `0 3 * * *` (daily at 03:00 UTC ≈ 00:00 America/Sao_Paulo)
- **Source API:** `https://api.openbrewerydb.org/v1/breweries` (with pagination)
- 

# Architecture (Medallion)

Bronze (Raw → GCS NDJSON → BigQuery)

Fetches all pages from the API (pagination, per_page=200).

Writes a single NDJSON file (UTF-8, ensure_ascii=False) to GCS.

Computes a SHA256 of the NDJSON and keeps a checkpoint in GCS.
Silver/Gold only run if the hash changed (incremental gate).

Force run options:

Airflow Variable bees_force=true, or

Trigger with run config {"force": true}.

## GCS Paths

**Bronze**
- `gs://us-central1-composer-case-165cfec3-bucket/data/bronze/breweries.ndjson`
- `gs://us-central1-composer-case-165cfec3-bucket/data/bronze/archive/breweries_<timestamp>.ndjson`
- `gs://us-central1-composer-case-165cfec3-bucket/logs/<timestamp>/bronze.log`

**Silver**
- `gs://us-central1-composer-case-165cfec3-bucket/data/silver/breweries_transformed.parquet`
- `gs://us-central1-composer-case-165cfec3-bucket/data/silver/archive/breweries_transformed_<timestamp>.parquet`
- `gs://us-central1-composer-case-165cfec3-bucket/logs/<timestamp>/silver.log`

**Gold**
- `gs://us-central1-composer-case-165cfec3-bucket/data/gold/breweries_aggregated.parquet`
- `gs://us-central1-composer-case-165cfec3-bucket/data/gold/archive/breweries_aggregated_<timestamp>.parquet`
- `gs://us-central1-composer-case-165cfec3-bucket/logs/<timestamp>/gold.log`

**Control (Bronze hash gate)**
- `gs://us-central1-composer-case-165cfec3-bucket/control/bronze_sha256.txt`

> `<timestamp>` format: `YYYYMMDDTHHMMSSZ` (UTC).


# Big Query Interface:

case-abinbev-469918.Medallion.bronze

Schema mirrors the API payload (STRING fields for raw text):
id, name, brewery_type, address_1, address_2, address_3, city, state_province, postal_code, country, longitude, latitude, phone, website_url, state, street.

Silver (Clean/Normalize → Parquet → BigQuery)

Reads Bronze from BigQuery and applies text repairs to fix known mojibake issues from the source (e.g., Caf�→Café, Stra�e→Straße, K�rnten→Kärnten, Nieder�sterreich→Niederösterreich, W�rthersee→Wörthersee, Wimitzbr�u→Wimitzbräu).
These are hard-coded in the DAG so the analytical layer is clean even if the API returns broken characters.

Normalizes Unicode to NFC, casts longitude/latitude to FLOAT64.

Creates state_partition (deterministic integer) for range partitioning.


# DDL

# DATASET

CREATE SCHEMA IF NOT EXISTS `case-abinbev-469918.Medallion`;

# BRONZE

CREATE TABLE IF NOT EXISTS `case-abinbev-469918.Medallion.bronze` (
  id            STRING,
  name          STRING,
  brewery_type  STRING,
  address_1     STRING,
  address_2     STRING,
  address_3     STRING,
  city          STRING,
  state_province STRING,
  postal_code   STRING,
  country       STRING,
  longitude     STRING,  -- kept as STRING in bronze (raw)
  latitude      STRING,  -- kept as STRING in bronze (raw)
  phone         STRING,
  website_url   STRING,
  state         STRING,
  street        STRING
);

# SILVER

 CREATE TABLE IF NOT EXISTS `case-abinbev-469918.Medallion.silver` (
  id STRING NOT NULL,
  name STRING NOT NULL,
  brewery_type STRING,
  address_1 STRING,
  address_2 STRING,
  address_3 STRING,
  city STRING NOT NULL,
  state STRING,
  state_province STRING,
  street STRING,
  postal_code STRING,
  country STRING NOT NULL,
  longitude FLOAT64,
  latitude FLOAT64,
  phone STRING,
  website_url STRING,
  state_partition INT64
)
PARTITION BY RANGE_BUCKET(state_partition, GENERATE_ARRAY(0, 50, 1))
CLUSTER BY country, city;

# GOLD

CREATE TABLE IF NOT EXISTS `case-abinbev-469918.Medallion.gold` (
  country         STRING,  -- Country name (from Silver)
  state           STRING,  -- State/Province (from Silver)
  brewery_type    STRING,  -- Type (e.g., micro, brewpub, large, bar, etc.)
  total_breweries INT64    -- Aggregated count
)
CLUSTER BY country, state, brewery_type;

**Columns**

country (STRING): Country of the brewery.

state (STRING): State/Province (already normalized in Silver).

brewery_type (STRING): Category from the source API (e.g., micro, brewpub, large, bar).

total_breweries (INT64): Count of rows per (country, state, brewery_type).

**Populated by**

-- Conceptual aggregation used by the pipeline:
SELECT
  country,
  state,
  brewery_type,
  COUNT(*) AS total_breweries
FROM `case-abinbev-469918.Medallion.silver`
GROUP BY 1,2,3;

# Gold Sample

SELECT country, state, brewery_type, total_breweries
FROM `case-abinbev-469918.Medallion.gold`
ORDER BY country, state, brewery_type;


## How to deploy & run

**Upload the DAG**
- Put `dag.py` in: `gs://us-central1-composer-case-165cfec3-bucket/dags/dag.py`

**PyPI packages (Composer 3)**
- Install via Composer UI (PyPI packages tab) if needed:
  - `google-cloud-storage`, `google-cloud-bigquery`, `pandas`, `pyarrow`, `requests`, `pendulum`

**Trigger manually (Cloud Shell)**
```bash
gcloud composer environments run composer-case --location=us-central1 \
  dags trigger -- bees_breweries_daily --conf='{"force": true}'

Force full rebuild (alternative)

In Airflow → Admin → Variables set bees_force=true and trigger normally.

To reset the change gate:
gsutil rm -f gs://us-central1-composer-case-165cfec3-bucket/control/bronze_sha256.txt

# Validation queries

**Row counts**
```sql
SELECT 'bronze' AS tbl, COUNT(*) FROM `case-abinbev-469918.Medallion.bronze`
UNION ALL SELECT 'silver', COUNT(*) FROM `case-abinbev-469918.Medallion.silver`
UNION ALL SELECT 'gold',   COUNT(*) FROM `case-abinbev-469918.Medallion.gold`;


Writes Parquet to GCS and loads into Silver (WRITE_TRUNCATE).
