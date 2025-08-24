# dag.py  — BEES Breweries (single DAG: bronze -> silver -> gold)
# Language: EN (hard-coded fixes for Austrian strings in Silver)
from __future__ import annotations

import json
import hashlib
import io
import re
import unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# -----------------------
# CONFIG
# -----------------------
PROJECT_ID = "case-abinbev-469918"
BQ_DATASET = "Medallion"
BRONZE_TABLE = f"{BQ_DATASET}.bronze"
SILVER_TABLE = f"{BQ_DATASET}.silver"
GOLD_TABLE = f"{BQ_DATASET}.gold"

# Composer bucket where we keep data, logs and control files
GCS_BUCKET = "us-central1-composer-case-165cfec3-bucket"
GCS_PREFIX_DATA = "data"
GCS_PREFIX_LOGS = "logs"
GCS_PREFIX_CTRL = "control"

API_BASE = "https://api.openbrewerydb.org/v1/breweries"
API_PER_PAGE = 200  # max
API_TIMEOUT = 30

# Scheduling: once per day by default
SCHEDULE = "0 3 * * *"  # 03:00 UTC daily

# -----------------------
# HELPERS
# -----------------------
def _now() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _gcs() -> storage.Client:
    return storage.Client(project=PROJECT_ID)

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)

def _blob(path: str) -> storage.Blob:
    return _gcs().bucket(GCS_BUCKET).blob(path)

def _gcs_write_text(bucket: str, path: str, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
    blob = _gcs().bucket(bucket).blob(path)
    blob.upload_from_string(text, content_type=content_type)

def _gcs_write_bytes(bucket: str, path: str, data: bytes, content_type: str) -> None:
    blob = _gcs().bucket(bucket).blob(path)
    blob.upload_from_string(data, content_type=content_type)

def _gcs_read_text(bucket: str, path: str) -> Optional[str]:
    blob = _gcs().bucket(bucket).blob(path)
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _bool_from_var(name: str, default: bool = False) -> bool:
    try:
        val = Variable.get(name)
        return str(val).lower() in ("1", "true", "yes", "y")
    except Exception:
        return default

def _log(msg: str) -> None:
    print(msg, flush=True)

# -----------------------
# BRONZE
# -----------------------
def bronze_extract_and_gate(**context) -> bool:
    """
    Fetch all breweries with pagination, write a single NDJSON in UTF-8 to GCS,
    compute sha256 and compare with last execution. Returns True when changed.
    """
    import requests

    ts = _now()
    force = (_bool_from_var("bees_force", False) or
             (context.get("dag_run") and context["dag_run"].conf and context["dag_run"].conf.get("force") is True))

    rows: List[Dict[str, Any]] = []
    page = 1
    total_pages = 0

    while True:
        url = f"{API_BASE}?page={page}&per_page={API_PER_PAGE}"
        r = requests.get(url, timeout=API_TIMEOUT)
        r.raise_for_status()
        batch = r.json()  # already decoded as unicode
        if not batch:
            break
        rows.extend(batch)
        total_pages += 1
        _log(f"[BRONZE] fetched page {page:05d} ({len(batch)} rows)")
        page += 1

    # Build NDJSON bytes with ensure_ascii=False and explicit UTF-8
    ndjson = "\n".join(json.dumps(rec, ensure_ascii=False) for rec in rows) + ("\n" if rows else "")
    ndjson_bytes = ndjson.encode("utf-8")
    sha = _sha256_bytes(ndjson_bytes)

    # Paths
    bronze_cur = f"{GCS_PREFIX_DATA}/bronze/breweries.ndjson"
    bronze_arc = f"{GCS_PREFIX_DATA}/bronze/archive/breweries_{ts}.ndjson"
    ctrl_path = f"{GCS_PREFIX_CTRL}/bronze_sha256.txt"
    log_path = f"{GCS_PREFIX_LOGS}/{ts}/bronze.log"

    # Compare with previous hash
    last_sha = _gcs_read_text(GCS_BUCKET, ctrl_path)
    changed = (last_sha != sha) or force

    if changed:
        # write current and archive copies
        _gcs_write_bytes(GCS_BUCKET, bronze_cur, ndjson_bytes, content_type="application/x-ndjson; charset=utf-8")
        _gcs_write_bytes(GCS_BUCKET, bronze_arc, ndjson_bytes, content_type="application/x-ndjson; charset=utf-8")
        _gcs_write_text(GCS_BUCKET, ctrl_path, sha)
        _gcs_write_text(GCS_BUCKET, log_path, f"Change detected. pages={total_pages}, rows={len(rows)}, sha256={sha}")
        _log(f"[BRONZE] CHANGED pages={total_pages} rows={len(rows)} sha256={sha}")
    else:
        _gcs_write_text(GCS_BUCKET, log_path, f"No change. pages={total_pages}, rows={len(rows)}, sha256={sha}")
        _log(f"[BRONZE] NO CHANGE pages={total_pages} rows={len(rows)} sha256={sha}")

    return changed

def bronze_load_to_bigquery(**_):
    """
    Load NDJSON from GCS into BigQuery table Medallion.bronze (truncate).
    """
    bq = _bq()
    uri = f"gs://{GCS_BUCKET}/{GCS_PREFIX_DATA}/bronze/breweries.ndjson"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
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
        ],
    )

    job = bq.load_table_from_uri(uri, f"{PROJECT_ID}.{BRONZE_TABLE}", job_config=job_config)
    job.result()
    _log(f"[BRONZE] loaded into {BRONZE_TABLE}")

# -----------------------
# SILVER — HARD-CODED FIXES FOR AUSTRIAN STRINGS
# -----------------------

# Exact replacements you asked for (mojibake -> correct)
# Add new ones here if you find more
REPLACE_WORDS = {
    "Caf�": "Café",
    "Stra�e": "Straße",
    "stra�e": "straße",
    "Nieder�sterreich": "Niederösterreich",
    "Ober�sterreich": "Oberösterreich",
    "K�rnten": "Kärnten",
    "W�rthersee": "Wörthersee",
}

# Regexes for common patterns like "...-Stra�e" etc.
REPLACE_REGEX = [
    (re.compile(r"([Ss])tra�e"), lambda m: "Straße" if m.group(1) == "S" else "straße"),
]

def fix_text(x: Any) -> Any:
    """
    Repair visible mojibake from bronze so that BigQuery shows
    proper characters (Café, Straße, Niederösterreich, Kärnten, …).

    1) run specific replacements (words)
    2) run regex fixes for Straße variants
    3) normalize to NFC
    """
    if x is None:
        return x
    if not isinstance(x, str):
        return x

    s = x
    # specific words
    for bad, good in REPLACE_WORDS.items():
        if bad in s:
            s = s.replace(bad, good)
    # regex patterns
    for rx, repl in REPLACE_REGEX:
        s = rx.sub(repl, s)

    # final unicode normalization (does not change ascii)
    s = unicodedata.normalize("NFC", s)
    return s

def silver_transform_and_save_parquet(**_):
    """
    Read bronze from BQ -> clean mojibake -> cast columns -> write parquet to GCS,
    then load into Medallion.silver (truncate).
    """
    bq = _bq()
    gcs = _gcs()
    ts = _now()

    # Read bronze
    sql = f"SELECT * FROM `{PROJECT_ID}.{BRONZE_TABLE}`"
    df = bq.query(sql).result().to_dataframe(create_bqstorage_client=False)

    # Apply hard-coded text fixes to all object columns
    obj_cols = [c for c in df.columns if df[c].dtype == "object"]
    for c in obj_cols:
        df[c] = df[c].map(fix_text)

    # Longitude/Latitude -> floats (nullable)
    def to_float(s):
        try:
            return float(s) if s not in (None, "", "null") else None
        except Exception:
            return None

    df["longitude"] = df["longitude"].map(to_float)
    df["latitude"] = df["latitude"].map(to_float)

    # Optional: stable int partition 0..49 by state (same idea you used)
    import zlib
    def state_partition(val: Any) -> Optional[int]:
        if val is None or str(val).strip() == "":
            return None
        return zlib.crc32(str(val).encode("utf-8")) % 50

    df["state_partition"] = df["state"].map(state_partition)

    # Save parquet (current + archive)
    out_cur = f"{GCS_PREFIX_DATA}/silver/breweries_transformed.parquet"
    out_arc = f"{GCS_PREFIX_DATA}/silver/archive/breweries_transformed_{ts}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    gcs.bucket(GCS_BUCKET).blob(out_cur).upload_from_file(buf, content_type="application/octet-stream")
    buf.seek(0)
    gcs.bucket(GCS_BUCKET).blob(out_arc).upload_from_file(buf, content_type="application/octet-stream")

    # Load into BigQuery silver (truncate)
    job = bq.load_table_from_dataframe(
        df, f"{PROJECT_ID}.{SILVER_TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    _gcs_write_text(GCS_BUCKET, f"{GCS_PREFIX_LOGS}/{ts}/silver.log",
                    f"Silver rows={len(df)} (mojibake fixed)")
    _log(f"[SILVER] rows={len(df)} loaded into {SILVER_TABLE}")

# -----------------------
# GOLD
# -----------------------
def gold_aggregate_and_save(**_):
    """
    Aggregate breweries per country/state/brewery_type -> parquet on GCS + BQ gold.
    """
    bq = _bq()
    gcs = _gcs()
    ts = _now()

    sql = f"""
      SELECT
        country,
        state,
        brewery_type,
        COUNT(*) AS total_breweries
      FROM `{PROJECT_ID}.{SILVER_TABLE}`
      GROUP BY country, state, brewery_type
    """
    df = bq.query(sql).result().to_dataframe(create_bqstorage_client=False)

    out_cur = f"{GCS_PREFIX_DATA}/gold/breweries_aggregated.parquet"
    out_arc = f"{GCS_PREFIX_DATA}/gold/archive/breweries_aggregated_{ts}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    gcs.bucket(GCS_BUCKET).blob(out_cur).upload_from_file(buf, content_type="application/octet-stream")
    buf.seek(0)
    gcs.bucket(GCS_BUCKET).blob(out_arc).upload_from_file(buf, content_type="application/octet-stream")

    job = bq.load_table_from_dataframe(
        df, f"{PROJECT_ID}.{GOLD_TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    _gcs_write_text(GCS_BUCKET, f"{GCS_PREFIX_LOGS}/{ts}/gold.log",
                    f"Gold rows={len(df)}")
    _log(f"[GOLD] rows={len(df)} loaded into {GOLD_TABLE}")

# -----------------------
# DAG
# -----------------------
default_args = {
    "owner": "bees-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bees_breweries_daily",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["bees", "open-brewery", "medallion", "bronze-silver-gold"],
) as dag:

    bronze_gate = ShortCircuitOperator(
        task_id="bronze_extract_and_gate",
        python_callable=bronze_extract_and_gate,
    )

    bronze_load = PythonOperator(
        task_id="bronze_load_to_bigquery",
        python_callable=bronze_load_to_bigquery,
    )

    silver_task = PythonOperator(
        task_id="silver_transform_and_save_parquet",
        python_callable=silver_transform_and_save_parquet,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregate_and_save",
        python_callable=gold_aggregate_and_save,
    )

    bronze_gate >> bronze_load >> silver_task >> gold_task
