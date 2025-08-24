# dag.py — Single DAG (Bronze → Silver → Gold) with hard-coded fixes for Austrian mojibake
from __future__ import annotations

import hashlib
import io
import json
import re
import unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from google.cloud import bigquery, storage

# -----------------------
# CONFIG
# -----------------------
PROJECT_ID = "case-abinbev-469918"
BQ_DATASET = "Medallion"
BRONZE_TABLE = f"{BQ_DATASET}.bronze"
SILVER_TABLE = f"{BQ_DATASET}.silver"
GOLD_TABLE   = f"{BQ_DATASET}.gold"

GCS_BUCKET = "us-central1-composer-case-165cfec3-bucket"
GCS_PREFIX_DATA    = "data"
GCS_PREFIX_LOGS    = "logs"
GCS_PREFIX_CONTROL = "control"

API_BASE = "https://api.openbrewerydb.org/v1/breweries"
API_PER_PAGE = 200
API_TIMEOUT = 30

SCHEDULE = "0 3 * * *"  # daily 03:00 UTC

TEXT_COLS = [
    "name", "brewery_type", "address_1", "address_2", "address_3",
    "city", "state", "state_province", "street", "postal_code",
    "country", "phone", "website_url"
]

# -----------------------
# HELPERS
# -----------------------
def _now() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _gcs() -> storage.Client:
    return storage.Client(project=PROJECT_ID)

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)

def _gcs_write_text(path: str, text: str, content_type="text/plain; charset=utf-8"):
    _gcs().bucket(GCS_BUCKET).blob(path).upload_from_string(text, content_type=content_type)

def _gcs_write_bytes(path: str, data: bytes, content_type="application/octet-stream"):
    _gcs().bucket(GCS_BUCKET).blob(path).upload_from_string(data, content_type=content_type)

def _gcs_read_text(path: str) -> Optional[str]:
    b = _gcs().bucket(GCS_BUCKET).blob(path)
    if not b.exists():
        return None
    return b.download_as_text(encoding="utf-8")

def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _log(msg: str):  # prints still go to task logs; we also mirror key events to GCS files
    print(msg, flush=True)

# -----------------------
# BRONZE
# -----------------------
def bronze_extract_and_gate(**context) -> bool:
    """
    Fetch ALL pages → single NDJSON (UTF-8) in GCS → sha256 gate (with Variable 'bees_force').
    """
    ts = _now()
    force = (str(Variable.get("bees_force", default_var="false")).lower() == "true") \
            or bool((getattr(context.get("dag_run"), "conf", {}) or {}).get("force"))

    rows: List[Dict[str, Any]] = []
    page = 1
    total_pages = 0

    while True:
        url = f"{API_BASE}?page={page}&per_page={API_PER_PAGE}"
        r = requests.get(url, timeout=API_TIMEOUT)
        r.raise_for_status()
        batch = r.json()  # already Unicode
        if not batch:
            break
        rows.extend(batch)
        total_pages += 1
        _log(f"[BRONZE] fetched page {page:05d} ({len(batch)} rows)")
        page += 1

    ndjson = "\n".join(json.dumps(rec, ensure_ascii=False) for rec in rows) + ("\n" if rows else "")
    raw = ndjson.encode("utf-8")
    sha = _sha256(raw)

    cur = f"{GCS_PREFIX_DATA}/bronze/breweries.ndjson"
    arc = f"{GCS_PREFIX_DATA}/bronze/archive/breweries_{ts}.ndjson"
    ctrl = f"{GCS_PREFIX_CONTROL}/bronze_sha256.txt"
    logp = f"{GCS_PREFIX_LOGS}/{ts}/bronze.log"

    prev = _gcs_read_text(ctrl)
    changed = (prev != sha) or force

    if changed:
        _gcs_write_bytes(cur, raw, "application/x-ndjson; charset=utf-8")
        _gcs_write_bytes(arc, raw, "application/x-ndjson; charset=utf-8")
        _gcs_write_text(ctrl, sha)
        _gcs_write_text(logp, f"Change detected. pages={total_pages}, rows={len(rows)}, sha256={sha}")
        _log(f"[BRONZE] CHANGED pages={total_pages} rows={len(rows)} sha256={sha}")
    else:
        _gcs_write_text(logp, f"No change. pages={total_pages}, rows={len(rows)}, sha256={sha}")
        _log(f"[BRONZE] NO CHANGE pages={total_pages} rows={len(rows)} sha256={sha}")

    return changed

def bronze_load_to_bigquery(**_):
    bq = _bq()
    uri = f"gs://{GCS_BUCKET}/{GCS_PREFIX_DATA}/bronze/breweries.ndjson"
    job = bq.load_table_from_uri(
        uri,
        f"{PROJECT_ID}.{BRONZE_TABLE}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("id","STRING"),
                bigquery.SchemaField("name","STRING"),
                bigquery.SchemaField("brewery_type","STRING"),
                bigquery.SchemaField("address_1","STRING"),
                bigquery.SchemaField("address_2","STRING"),
                bigquery.SchemaField("address_3","STRING"),
                bigquery.SchemaField("city","STRING"),
                bigquery.SchemaField("state_province","STRING"),
                bigquery.SchemaField("postal_code","STRING"),
                bigquery.SchemaField("country","STRING"),
                bigquery.SchemaField("longitude","STRING"),
                bigquery.SchemaField("latitude","STRING"),
                bigquery.SchemaField("phone","STRING"),
                bigquery.SchemaField("website_url","STRING"),
                bigquery.SchemaField("state","STRING"),
                bigquery.SchemaField("street","STRING"),
            ],
        ),
    )
    job.result()
    _log(f"[BRONZE] loaded into {BRONZE_TABLE}")

# -----------------------
# SILVER — HARD-CODED FIXES
# -----------------------

# Exact tokens and full strings you reported
REPLACE_WORDS = {
    # exact business + address
    "Caf\uFFFD Okei": "Café Okei",                     # Caf� Okei
    "Feldkirchenstra\uFFFDe 40": "Feldkirchenstraße 40",# Feldkirchenstra�e 40

    # states / provinces / places
    "K\uFFFDrnten": "Kärnten",                          # K�rnten
    "Nieder\uFFFDsterreich": "Niederösterreich",       # Nieder�sterreich
    "Ober\uFFFDsterreich": "Oberösterreich",           # Ober�sterreich
    "W\uFFFDrthersee": "Wörthersee",                   # W�rthersee

    # common words / names
    "Stra\uFFFDe": "Straße",                            # Stra�e
    "stra\uFFFDe": "straße",                            # stra�e
    "Wimitzbr\uFFFDu": "Wimitzbräu",                    # Wimitzbr�u
    "Dr.-Beurle-Stra\uFFFDe": "Dr.-Beurle-Straße",
    "Mautner-Markhof-Stra\uFFFDe": "Mautner-Markhof-Straße",
    "Stiftstra\uFFFDe": "Stiftstraße",
}

# generic double-encoded sequences (Ã¶ → ö, etc.)
DOUBLE_ENC = {
    "Ã¶": "ö", "Ã–": "Ö",
    "Ã¤": "ä", "Ã„": "Ä",
    "Ã¼": "ü", "Ãœ": "Ü",
    "ÃŸ": "ß",
    "Ã©": "é", "ÃÈ": "È", "Ãª": "ê",
    "Ã¡": "á", "Ãà": "à",
    "Ã³": "ó", "Ã²": "ò", "Ã´": "ô",
    "Ã§": "ç", "Ãº": "ú", "Ã¹": "ù",
}

# Regexes para variantes (ex.: qualquer coisa + "stra�e" → "straße")
REPLACE_REGEX = [
    (re.compile(r"([Ss])tra\uFFFDe"), lambda m: "Straße" if m.group(1) == "S" else "straße"),
]

def fix_text(x: Any) -> Any:
    if x is None:
        return x
    if not isinstance(x, str):
        x = str(x)

    s = x

    # exact/full-word replacements first
    for bad, good in REPLACE_WORDS.items():
        if bad in s:
            s = s.replace(bad, good)

    # generic double-enc sequences
    for bad, good in DOUBLE_ENC.items():
        if bad in s:
            s = s.replace(bad, good)

    # regex patterns (straße variants using U+FFFD)
    for rx, repl in REPLACE_REGEX:
        s = rx.sub(repl, s)

    # normalize to NFC
    s = unicodedata.normalize("NFC", s)
    return s

def silver_transform_and_save_parquet(**_):
    bq = _bq()
    ts = _now()

    # Pull from bronze (raw)
    df = bq.query(f"SELECT * FROM `{PROJECT_ID}.{BRONZE_TABLE}`").result().to_dataframe(create_bqstorage_client=False)

    # Apply hard fixes to ALL known text columns explicitly (not dtype-based)
    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].map(fix_text)

    # Cast numeric columns
    def to_float(v):
        try:
            return float(v) if v not in (None, "", "null") else None
        except Exception:
            return None
    if "longitude" in df.columns: df["longitude"] = df["longitude"].map(to_float)
    if "latitude"  in df.columns: df["latitude"]  = df["latitude"].map(to_float)

    # Simple deterministic partition (0..49) based on state / fallback state_province
    import zlib
    def part(row):
        base = row.get("state") or row.get("state_province")
        base = fix_text(base) if base else base
        if not base:
            return None
        return zlib.crc32(base.encode("utf-8")) % 50
    df["state_partition"] = df.apply(part, axis=1)

    # Save Parquet (current + archive)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    cur = f"{GCS_PREFIX_DATA}/silver/breweries_transformed.parquet"
    arc = f"{GCS_PREFIX_DATA}/silver/archive/breweries_transformed_{ts}.parquet"
    _gcs_write_bytes(cur, buf.getvalue())
    _gcs_write_bytes(arc, buf.getvalue())

    # Load into BigQuery (truncate)
    bq.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{SILVER_TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    _gcs_write_text(f"{GCS_PREFIX_LOGS}/{ts}/silver.log", f"Silver rows={len(df)} (hard fixes applied)")
    _log(f"[SILVER] rows={len(df)} → {SILVER_TABLE}")

# -----------------------
# GOLD
# -----------------------
def gold_aggregate_and_save(**_):
    bq = _bq()
    ts = _now()
    sql = f"""
      SELECT
        country,
        state,
        brewery_type,
        COUNT(*) AS total_breweries
      FROM `{PROJECT_ID}.{SILVER_TABLE}`
      GROUP BY country, state, brewery_type
      ORDER BY country, state, brewery_type
    """
    df = bq.query(sql).result().to_dataframe(create_bqstorage_client=False)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    cur = f"{GCS_PREFIX_DATA}/gold/breweries_aggregated.parquet"
    arc = f"{GCS_PREFIX_DATA}/gold/archive/breweries_aggregated_{ts}.parquet"
    _gcs_write_bytes(cur, buf.getvalue())
    _gcs_write_bytes(arc, buf.getvalue())

    bq.load_table_from_dataframe(
        df, f"{PROJECT_ID}.{GOLD_TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    ).result()

    _gcs_write_text(f"{GCS_PREFIX_LOGS}/{ts}/gold.log", f"Gold rows={len(df)}")
    _log(f"[GOLD] rows={len(df)} → {GOLD_TABLE}")

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
    description="Open Brewery DB → Medallion (Bronze/Silver/Gold) with explicit Austrian fixes in Silver",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["bees","open-brewery","medallion"],
) as dag:

    bronze_gate = ShortCircuitOperator(
        task_id="bronze_extract_and_gate",
        python_callable=bronze_extract_and_gate,
    )

    bronze_load = PythonOperator(
        task_id="bronze_load_to_bigquery",
        python_callable=bronze_load_to_bigquery,
    )

    silver = PythonOperator(
        task_id="silver_transform_and_save_parquet",
        python_callable=silver_transform_and_save_parquet,
    )

    gold = PythonOperator(
        task_id="gold_aggregate_and_save",
        python_callable=gold_aggregate_and_save,
    )

    bronze_gate >> bronze_load >> silver >> gold
