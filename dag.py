# dag.py
# Single DAG for BEES Breweries case — Bronze/Silver/Gold
# Composer 3 / Airflow 2.x

from __future__ import annotations

import hashlib
import io
import json
import os
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
import pendulum
import requests

from google.cloud import bigquery
from google.cloud import storage


# ────────────────────────────────────────────────────────────────────────────────
# Configuration (adjust only if you really need to)
# ────────────────────────────────────────────────────────────────────────────────

PROJECT_ID = "case-abinbev-469918"

BQ_DATASET = "Medallion"
BQ_TABLE_BRONZE = f"{BQ_DATASET}.bronze"
BQ_TABLE_SILVER = f"{BQ_DATASET}.silver"
BQ_TABLE_GOLD = f"{BQ_DATASET}.gold"

# Composer bucket where we write data, logs, and control files
GCS_BUCKET = "us-central1-composer-case-165cfec3-bucket"

# Folders inside the bucket
GCS_PREFIX_DATA = "data"
GCS_PREFIX_LOGS = "logs"
GCS_PREFIX_CONTROL = "control"

# Open Brewery DB endpoint (no API key required)
OBD_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

# Scheduling
SCHEDULE = "0 3 * * *"  # once per day at 03:00 UTC
START_DATE = days_ago(1)

# Pagination
PER_PAGE = 200
PAGE_SLEEP_SEC = 0.05  # tiny pause to be nice to the API

# Integer partition range for Silver table
STATE_PARTITION_START = 0
STATE_PARTITION_END = 50  # inclusive
STATE_PARTITION_BUCKETS = STATE_PARTITION_END - STATE_PARTITION_START + 1


# ────────────────────────────────────────────────────────────────────────────────
# Utilities
# ────────────────────────────────────────────────────────────────────────────────

def gcs_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)

def bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)

def _gcs_write_text(bucket: str, blob_name: str, text: str) -> str:
    cli = gcs_client()
    blob = cli.bucket(bucket).blob(blob_name)
    blob.upload_from_string(text, content_type="text/plain; charset=utf-8")
    return f"gs://{bucket}/{blob_name}"

def _gcs_write_bytes(bucket: str, blob_name: str, data: bytes, content_type: str) -> str:
    cli = gcs_client()
    blob = cli.bucket(bucket).blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket}/{blob_name}"

def _gcs_read_text_if_exists(bucket: str, blob_name: str) -> Optional[str]:
    cli = gcs_client()
    blob = cli.bucket(bucket).blob(blob_name)
    if not blob.exists():
        return None
    return blob.download_as_text()

def _now_stamp() -> str:
    # e.g., 20250823T202349Z — stable for organizing logs
    return pendulum.now("UTC").format("YYYYMMDDTHHmmss[Z]")

def _normalize_str(s: str) -> str:
    # Normalize to NFC (keeps öäüß éèç… correct)
    return unicodedata.normalize("NFC", s)

def _normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in rec.items():
        if isinstance(v, str):
            out[k] = _normalize_str(v.strip())
        else:
            out[k] = v
    return out

def _decode_json_best_effort(b: bytes) -> Any:
    """
    Decode bytes -> JSON using strict UTF-8; if it fails or introduces
    U+FFFD (�), fall back to latin-1. Returns Python object (list/dict).
    """
    try:
        txt = b.decode("utf-8")
        if "\ufffd" not in txt:
            return json.loads(txt)
    except UnicodeDecodeError:
        pass
    # Fallback
    txt_l1 = b.decode("latin-1")
    return json.loads(txt_l1)

def _stable_int_partition(key: Optional[str]) -> int:
    if not key:
        return 0
    # Use SHA1 -> int -> modulo into [0..N-1]
    h = int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16)
    return STATE_PARTITION_START + (h % STATE_PARTITION_BUCKETS)

def _bronze_schema_bq() -> List[bigquery.SchemaField]:
    return [
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

def _ensure_dataset():
    cli = bq_client()
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
    try:
        cli.get_dataset(ds_ref)
    except Exception:
        ds_ref.location = "US"
        cli.create_dataset(ds_ref, exists_ok=True)

def _ensure_silver_table():
    cli = bq_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLE_SILVER}"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("brewery_type", "STRING"),
        bigquery.SchemaField("address_1", "STRING"),
        bigquery.SchemaField("address_2", "STRING"),
        bigquery.SchemaField("address_3", "STRING"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("state_province", "STRING"),
        bigquery.SchemaField("street", "STRING"),
        bigquery.SchemaField("postal_code", "STRING"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("website_url", "STRING"),
        bigquery.SchemaField("state_partition", "INTEGER"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.range_partitioning = bigquery.RangePartitioning(
        field="state_partition",
        range_=bigquery.PartitionRange(
            start=STATE_PARTITION_START, end=STATE_PARTITION_END, interval=1
        ),
    )
    table.clustering_fields = ["country", "city"]
    cli.create_table(table, exists_ok=True)

def _ensure_gold_table():
    cli = bq_client()
    table_id = f"{PROJECT_ID}.{BQ_TABLE_GOLD}"
    schema = [
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("brewery_type", "STRING"),
        bigquery.SchemaField("total_breweries", "INTEGER"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.clustering_fields = ["country", "state", "brewery_type"]
    cli.create_table(table, exists_ok=True)


# ────────────────────────────────────────────────────────────────────────────────
# Bronze: fetch, save to GCS, gate by hash, and load to BigQuery
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class BronzeResult:
    proceed: bool
    sha256: str
    total_pages: int
    total_rows: int
    ndjson_gs_uri: Optional[str] = None
    log_gs_uri: Optional[str] = None
    meta_gs_uri: Optional[str] = None

def _fetch_all_pages() -> Tuple[List[dict], int]:
    session = requests.Session()
    session.headers.update(
        {"Accept": "application/json", "User-Agent": "bees-breweries-medallion/1.1"}
    )
    rows: List[dict] = []
    page = 1
    total_pages = 0

    while True:
        url = f"{OBD_BASE_URL}?per_page={PER_PAGE}&page={page}"
        r = session.get(url, timeout=30)
        r.raise_for_status()
        data = _decode_json_best_effort(r.content)
        if not data:
            break
        # Normalize strings
        data = [_normalize_record(d) for d in data]
        rows.extend(data)
        total_pages += 1
        page += 1
        time.sleep(PAGE_SLEEP_SEC)

    return rows, total_pages

def bronze_extract_and_gate(**context) -> BronzeResult:
    run_id = context["run_id"]
    ts = _now_stamp()
    log_lines: List[str] = []
    log = lambda m: log_lines.append(m)

    # Force flag from Airflow Variable (string "true"/"false")
    force_var = (Variable.get("bees_force", default_var="false") or "").lower() == "true"
    # Also allow dag_run.conf {"force": true} if your UI supports it
    dag_conf = (context.get("dag_run") or {}).conf or {}
    force_conf = bool(dag_conf.get("force"))
    force = force_var or force_conf

    log(f"[{ts}] Starting Bronze fetch. force={force}")

    rows, total_pages = _fetch_all_pages()
    total_rows = len(rows)

    # Compute SHA256 over the **canonical NDJSON bytes** so we detect real changes
    ndjson_buf = io.StringIO()
    for r in rows:
        ndjson_buf.write(json.dumps(r, ensure_ascii=False))
        ndjson_buf.write("\n")
    ndjson_txt = ndjson_buf.getvalue()
    ndjson_bytes = ndjson_txt.encode("utf-8")
    sha256 = hashlib.sha256(ndjson_bytes).hexdigest()
    log(f"Fetched pages={total_pages} rows={total_rows} sha256={sha256}")

    # Compare with control file
    control_blob = f"{GCS_PREFIX_CONTROL}/bronze_sha256.txt"
    prev_sha = _gcs_read_text_if_exists(GCS_BUCKET, control_blob)
    if prev_sha:
        log(f"Previous sha256={prev_sha.strip()}")
    else:
        log("No previous sha256 found (first run).")

    # If unchanged and not forced, write log and return 'skip'
    if (prev_sha or "") == sha256 and not force:
        log("No change detected. Downstream will be skipped.")
        log_uri = _gcs_write_text(GCS_BUCKET, f"{GCS_PREFIX_LOGS}/{ts}/bronze.log", "\n".join(log_lines))
        meta_uri = _gcs_write_text(
            GCS_BUCKET,
            f"{GCS_PREFIX_LOGS}/{ts}/bronze_meta.json",
            json.dumps({"pages": total_pages, "rows": total_rows, "sha256": sha256, "proceed": False}, ensure_ascii=False, indent=2),
        )
        return BronzeResult(False, sha256, total_pages, total_rows, None, log_uri, meta_uri)

    # We proceed: write NDJSON (current and an archive copy), control and logs
    ndjson_blob = f"{GCS_PREFIX_DATA}/bronze/breweries.ndjson"
    ndjson_arch_blob = f"{GCS_PREFIX_DATA}/bronze/archive/breweries_{ts}.ndjson"
    ndjson_uri = _gcs_write_bytes(GCS_BUCKET, ndjson_blob, ndjson_bytes, "application/x-ndjson; charset=utf-8")
    _gcs_write_bytes(GCS_BUCKET, ndjson_arch_blob, ndjson_bytes, "application/x-ndjson; charset=utf-8")
    _gcs_write_text(GCS_BUCKET, control_blob, sha256)

    log("Saved NDJSON to:")
    log(f" - {ndjson_uri}")
    log(f" - gs://{GCS_BUCKET}/{ndjson_arch_blob}")

    log_uri = _gcs_write_text(GCS_BUCKET, f"{GCS_PREFIX_LOGS}/{ts}/bronze.log", "\n".join(log_lines))
    meta_uri = _gcs_write_text(
        GCS_BUCKET,
        f"{GCS_PREFIX_LOGS}/{ts}/bronze_meta.json",
        json.dumps({"pages": total_pages, "rows": total_rows, "sha256": sha256, "proceed": True}, ensure_ascii=False, indent=2),
    )

    # Push XCom for downstream
    ti = context["ti"]
    ti.xcom_push(key="bronze_ndjson_uri", value=ndjson_uri)

    return BronzeResult(True, sha256, total_pages, total_rows, ndjson_uri, log_uri, meta_uri)

def bronze_load_to_bigquery(**context):
    # Ensure dataset and table exist
    _ensure_dataset()
    cli = bq_client()

    table_id = f"{PROJECT_ID}.{BQ_TABLE_BRONZE}"
    schema = _bronze_schema_bq()

    # Create the table if needed
    table = bigquery.Table(table_id, schema=schema)
    cli.create_table(table, exists_ok=True)

    # Load from the NDJSON we just wrote
    ti = context["ti"]
    ndjson_uri: str = ti.xcom_pull(key="bronze_ndjson_uri", task_ids="bronze_extract_and_gate")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = cli.load_table_from_uri(ndjson_uri, table_id, job_config=job_config)
    load_job.result()


# ────────────────────────────────────────────────────────────────────────────────
# Silver: transform, write Parquet to GCS and load to BQ (integer partitioned)
# ────────────────────────────────────────────────────────────────────────────────

def silver_transform_and_save_parquet(**context):
    _ensure_dataset()
    _ensure_silver_table()

    cli = bq_client()
    query = f"""
        SELECT
            id,
            name,
            brewery_type,
            address_1, address_2, address_3,
            city,
            state,
            state_province,
            street,
            postal_code,
            country,
            SAFE_CAST(NULLIF(longitude, '') AS FLOAT64) AS longitude,
            SAFE_CAST(NULLIF(latitude, '') AS FLOAT64)  AS latitude,
            phone,
            website_url
        FROM `{PROJECT_ID}.{BQ_TABLE_BRONZE}`
    """
    df = cli.query(query).result().to_dataframe(create_bqstorage_client=False)

    # Normalize key text columns again (paranoia)
    for col in ["name", "city", "state", "state_province", "country", "street", "address_1", "address_2", "address_3"]:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").map(lambda s: _normalize_str(str(s)) if s else None)

    # Deterministic integer partition
    df["state_partition"] = df["state"].fillna(df["state_province"]).map(lambda s: _stable_int_partition(s if pd.notna(s) else None))

    # Save Parquet to GCS
    ts = _now_stamp()
    silver_blob = f"{GCS_PREFIX_DATA}/silver/breweries_transformed.parquet"
    silver_arch = f"{GCS_PREFIX_DATA}/silver/archive/breweries_transformed_{ts}.parquet"

    # Use pyarrow engine
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    parquet_bytes = buf.getvalue()

    _gcs_write_bytes(GCS_BUCKET, silver_blob, parquet_bytes, "application/octet-stream")
    _gcs_write_bytes(GCS_BUCKET, silver_arch, parquet_bytes, "application/octet-stream")

    # Load to BigQuery (partitioned int range table)
    table_id = f"{PROJECT_ID}.{BQ_TABLE_SILVER}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    gcs_uri = f"gs://{GCS_BUCKET}/{silver_blob}"
    load_job = cli.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


# ────────────────────────────────────────────────────────────────────────────────
# Gold: aggregate and write Parquet + load to BQ
# ────────────────────────────────────────────────────────────────────────────────

def gold_aggregate_and_save(**context):
    _ensure_dataset()
    _ensure_gold_table()

    cli = bq_client()
    query = f"""
        SELECT
          country,
          COALESCE(NULLIF(state, ''), NULLIF(state_province, '')) AS state,
          brewery_type,
          COUNT(1) AS total_breweries
        FROM `{PROJECT_ID}.{BQ_TABLE_SILVER}`
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """
    df = cli.query(query).result().to_dataframe(create_bqstorage_client=False)

    # Parquet to GCS
    ts = _now_stamp()
    gold_blob = f"{GCS_PREFIX_DATA}/gold/breweries_aggregated.parquet"
    gold_arch = f"{GCS_PREFIX_DATA}/gold/archive/breweries_aggregated_{ts}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    parquet_bytes = buf.getvalue()

    _gcs_write_bytes(GCS_BUCKET, gold_blob, parquet_bytes, "application/octet-stream")
    _gcs_write_bytes(GCS_BUCKET, gold_arch, parquet_bytes, "application/octet-stream")

    # Load to BQ (clustered table)
    table_id = f"{PROJECT_ID}.{BQ_TABLE_GOLD}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    gcs_uri = f"gs://{GCS_BUCKET}/{gold_blob}"
    load_job = cli.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


# ────────────────────────────────────────────────────────────────────────────────
# DAG definition
# ────────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bees_breweries_daily",
    description="Open Brewery DB → Bronze/Silver/Gold with pagination, gating and BQ loads",
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["breweries", "medallion", "bees"],
) as dag:

    bronze_extract_and_gate_task = ShortCircuitOperator(
        task_id="bronze_extract_and_gate",
        python_callable=bronze_extract_and_gate,
        do_xcom_push=True,
    )

    bronze_load_to_bq_task = PythonOperator(
        task_id="bronze_load_to_bigquery",
        python_callable=bronze_load_to_bigquery,
    )

    silver_transform_and_save_task = PythonOperator(
        task_id="silver_transform_and_save_parquet",
        python_callable=silver_transform_and_save_parquet,
    )

    gold_aggregate_and_save_task = PythonOperator(
        task_id="gold_aggregate_and_save",
        python_callable=gold_aggregate_and_save,
    )

    # Flow
    bronze_extract_and_gate_task >> bronze_load_to_bq_task >> silver_transform_and_save_task >> gold_aggregate_and_save_task
