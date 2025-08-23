# dag.py — BEES Breweries (Bronze → Silver → Gold) — Single DAG
# Airflow 2.9+/2.10 (Composer 3)
from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# ============================================================================
# Config (override via Admin → Variables if needed)
# ============================================================================
PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="case-abinbev-469918")
DATASET    = Variable.get("BQ_DATASET",     default_var="Medallion")

# Use the Composer environment bucket for data/logs by default:
BUCKET_DATA         = Variable.get(
    "BUCKET_NAME",
    default_var="us-central1-composer-case-165cfec3-bucket",
)
COMPOSER_LOG_BUCKET = Variable.get("COMPOSER_LOG_BUCKET", default_var=BUCKET_DATA)

# Paths/prefixes
DATA_PREFIX  = "data"
BRONZE_DIR   = f"{DATA_PREFIX}/bronze"
SILVER_DIR   = f"{DATA_PREFIX}/silver"
GOLD_DIR     = f"{DATA_PREFIX}/gold"
CONTROL_BLOB = f"{BRONZE_DIR}/_control/last_sha256.txt"   # hash control file

# API (iterate pages until exhausted)
API_URL   = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE  = 200  # API maximum
TIMEOUT_S = 30

# DAG params
SCHEDULE     = "@daily"            # run daily; you can always trigger manually
START_DATE   = datetime(2024, 1, 1)
RETRIES      = 3
RETRY_DELAY  = timedelta(minutes=2)

# ============================================================================
# Helpers
# ============================================================================

def _save_log(lines: list[str]) -> None:
    """Append a run log into GCS (UTF-8)."""
    from google.cloud import storage
    content = "\n".join(lines) + "\n"
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    blob_path = f"logs/bronze_dag_log_{ts}.log"
    storage.Client().bucket(COMPOSER_LOG_BUCKET).blob(blob_path).upload_from_string(
        content, content_type="text/plain; charset=utf-8"
    )
    logging.info("Log written to gs://%s/%s", COMPOSER_LOG_BUCKET, blob_path)


def _read_last_hash(storage_client) -> str | None:
    try:
        return (
            storage_client.bucket(BUCKET_DATA)
            .blob(CONTROL_BLOB)
            .download_as_text(encoding="utf-8")
            .strip()
        )
    except Exception:
        return None


def _write_last_hash(storage_client, value: str) -> None:
    storage_client.bucket(BUCKET_DATA).blob(CONTROL_BLOB).upload_from_string(
        value + "\n", content_type="text/plain; charset=utf-8"
    )


def _http_get(params: dict, attempt: int):
    import requests
    headers = {"User-Agent": "bees-breweries/1.0"}
    r = requests.get(API_URL, params=params, headers=headers, timeout=TIMEOUT_S)
    if r.status_code in (429, 500, 502, 503, 504):
        backoff = min(60, 2**attempt) + 0.1 * attempt
        logging.warning("HTTP %s, retrying in %.1fs...", r.status_code, backoff)
        time.sleep(backoff)
    r.raise_for_status()
    return r


def _upload_page_jsonl(storage_client, bucket: str, path: str, rows: list[dict]) -> bytes:
    """
    Write one NDJSON page (true UTF-8). Returns the exact bytes written
    so we can compose a stable SHA256 across the whole run.
    """
    jsonl_bytes = b"".join(
        (json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        for r in rows
    )
    storage_client.bucket(bucket).blob(path).upload_from_string(
        jsonl_bytes, content_type="application/x-ndjson; charset=utf-8"
    )
    return jsonl_bytes


# ============================================================================
# BRONZE
# ============================================================================

def bronze_extract_and_gate(ds: str, **context) -> bool:
    """
    Fetch ALL pages (pagination) and store NDJSON into:
      gs://<bucket>/data/bronze/run_date=YYYY-MM-DD/page=00001.jsonl
    Compute SHA256 over the stored bytes and compare with last control hash.
    If changed (or conf {"force": true}), return True → downstream runs.
    """
    from google.cloud import storage

    conf = context.get("dag_run").conf or {}
    force = bool(conf.get("force"))

    sc = storage.Client()
    total_rows = 0
    total_pages = 0
    sha = hashlib.sha256()

    page = 1
    logs = []

    while True:
        params = {"page": page, "per_page": PER_PAGE}
        resp = _http_get(params, attempt=page)  # attempt used for backoff variability
        data = resp.json()
        if not data:
            break

        page_path = f"{BRONZE_DIR}/run_date={ds}/page={page:05d}.jsonl"
        page_bytes = _upload_page_jsonl(sc, BUCKET_DATA, page_path, data)
        sha.update(page_bytes)

        total_pages += 1
        total_rows += len(data)
        logging.info("Saved page %05d (%d rows)", page, len(data))
        logs.append(f"Saved page {page:05d} ({len(data)} rows) → gs://{BUCKET_DATA}/{page_path}")

        if len(data) < PER_PAGE:
            break
        page += 1

    current_hash = sha.hexdigest()
    prev_hash = _read_last_hash(sc)

    logs.append(f"Total pages: {total_pages} | rows: {total_rows} | sha256: {current_hash}")
    logs.append(f"Previous hash: {prev_hash or '(none)'}")

    changed = force or (current_hash != prev_hash)
    if changed:
        _write_last_hash(sc, current_hash)
        logs.append("Change detected (or force=true) → continue.")
    else:
        logs.append("No change → skip downstream.")

    _save_log(logs)
    return changed


def bronze_load_to_bigquery(ds: str, **_):
    """
    Load BRONZE NDJSON into BigQuery table Medallion.bronze.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    uri = f"gs://{BUCKET_DATA}/{BRONZE_DIR}/run_date={ds}/page=*.jsonl"

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("brewery_type", "STRING"),
        bigquery.SchemaField("address_1", "STRING"),
        bigquery.SchemaField("address_2", "STRING"),
        bigquery.SchemaField("address_3", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state_province", "STRING"),
        bigquery.SchemaField("postal_code", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("longitude", "STRING"),
        bigquery.SchemaField("latitude", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("website_url", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("street", "STRING"),
    ]

    table_id = f"{PROJECT_ID}.{DATASET}.bronze"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    dest = client.get_table(table_id)
    logging.info("Bronze loaded: %s (%s rows)", table_id, dest.num_rows)


# ============================================================================
# SILVER
# ============================================================================

def silver_transform_and_save_parquet(ds: str, **_):
    """
    Create/refresh the SILVER table (integer-range partition + clustering),
    then export a Parquet copy to GCS.
    """
    from google.cloud import bigquery

    bq = bigquery.Client(project=PROJECT_ID)

    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.silver`
    PARTITION BY RANGE_BUCKET(state_partition, GENERATE_ARRAY(0, 50, 1))
    CLUSTER BY country, city AS
    SELECT
      CAST(id AS STRING)                            AS id,
      CAST(name AS STRING)                          AS name,
      CAST(brewery_type AS STRING)                  AS brewery_type,
      CAST(address_1 AS STRING)                     AS address_1,
      CAST(address_2 AS STRING)                     AS address_2,
      CAST(address_3 AS STRING)                     AS address_3,
      CAST(city AS STRING)                          AS city,
      CAST(state AS STRING)                         AS state,
      CAST(state_province AS STRING)                AS state_province,
      CAST(street AS STRING)                        AS street,
      CAST(postal_code AS STRING)                   AS postal_code,
      CAST(country AS STRING)                       AS country,
      SAFE_CAST(longitude AS FLOAT64)               AS longitude,
      SAFE_CAST(latitude  AS FLOAT64)               AS latitude,
      CAST(phone AS STRING)                         AS phone,
      CAST(website_url AS STRING)                   AS website_url,
      ABS(MOD(FARM_FINGERPRINT(LOWER(COALESCE(state,''))), 51)) AS state_partition
    FROM `{PROJECT_ID}.{DATASET}.bronze`
    WHERE name IS NOT NULL AND city IS NOT NULL AND country IS NOT NULL
    """
    bq.query(sql).result()

    # Export a Parquet copy (handy for debugging/external consumption)
    df = bq.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.silver`") \
            .result().to_dataframe(create_bqstorage_client=True)
    path_parquet = f"gs://{BUCKET_DATA}/{SILVER_DIR}/breweries_transformed/breweries_transformed.parquet"
    df.to_parquet(path_parquet, index=False)  # gcsfs/pyarrow handle GCS I/O

    _save_log([
        "Silver: partitioned & clustered table created/updated.",
        f"Silver Parquet: {path_parquet}",
        f"Silver rows: {len(df)}",
    ])


# ============================================================================
# GOLD
# ============================================================================

def gold_aggregate_and_save(ds: str, **_):
    """
    Aggregate SILVER into GOLD and export a Parquet snapshot to GCS.
    """
    from google.cloud import bigquery

    bq = bigquery.Client(project=PROJECT_ID)

    sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.gold`
    CLUSTER BY country, state, brewery_type AS
    SELECT
      country,
      state,
      brewery_type,
      COUNT(*) AS total_breweries
    FROM `{PROJECT_ID}.{DATASET}.silver`
    GROUP BY country, state, brewery_type
    """
    bq.query(sql).result()

    df = bq.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.gold`") \
            .result().to_dataframe(create_bqstorage_client=True)
    path_parquet = f"gs://{BUCKET_DATA}/{GOLD_DIR}/breweries_aggregated.parquet"
    df.to_parquet(path_parquet, index=False)

    _save_log([
        "Gold: aggregated table created/updated.",
        f"Gold Parquet: {path_parquet}",
        f"Gold rows: {len(df)}",
    ])


# ============================================================================
# DAG definition
# ============================================================================

with DAG(
    dag_id="bees_breweries_daily",
    description="Open Brewery DB → Medallion (Bronze/Silver/Gold) with pagination, UTF-8, hash control & logs",
    start_date=START_DATE,
    schedule_interval=SCHEDULE,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": RETRIES,
        "retry_delay": RETRY_DELAY,
    },
    max_active_runs=1,
    tags=["bees", "medallion", "openbrewerydb"],
) as dag:

    bronze_extract_and_gate_task = ShortCircuitOperator(
        task_id="bronze_extract_and_gate",
        python_callable=bronze_extract_and_gate,
        op_kwargs={"ds": "{{ ds }}"},
    )

    bronze_load_task = PythonOperator(
        task_id="bronze_load_to_bigquery",
        python_callable=bronze_load_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )

    silver_task = PythonOperator(
        task_id="silver_transform_and_save_parquet",
        python_callable=silver_transform_and_save_parquet,
        op_kwargs={"ds": "{{ ds }}"},
    )

    gold_task = PythonOperator(
        task_id="gold_aggregate_and_save",
        python_callable=gold_aggregate_and_save,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # If Bronze returns False (no change), downstream tasks are skipped
    bronze_extract_and_gate_task >> bronze_load_task >> silver_task >> gold_task
