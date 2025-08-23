# bees_breweries_daily.py
# DAG única com seções. Nesta entrega: ##### BRONZE #####
# Requisitos: google-cloud-storage, google-cloud-bigquery, pandas, pyarrow, requests, gcsfs

from datetime import datetime, timedelta
import hashlib, json, time, logging
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable

from google.cloud import storage, bigquery

# ------------------------------ Config ---------------------------------
# Pegamos de Airflow Variables (com defaults que batem no seu projeto/bucket)
PROJECT_ID   = Variable.get("GCP_PROJECT_ID", default_var="case-abinbev-469918")
BUCKET_DATA  = Variable.get("BUCKET_NAME",    default_var="bucket-case-abinbev")
DATASET      = Variable.get("BQ_DATASET",     default_var="Medallion")

# Opcional: bucket de logs do Composer (se quiser salvar um .log adicional)
COMPOSER_LOG_BUCKET = Variable.get("COMPOSER_LOG_BUCKET", default_var="")

API_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200  # máximo suportado
# -----------------------------------------------------------------------

def _save_log(lines: list[str]) -> None:
    """Salva um log .log no bucket do Composer (opcional)."""
    if not COMPOSER_LOG_BUCKET:
        for ln in lines: logging.info(ln)
        return
    storage.Client().bucket(COMPOSER_LOG_BUCKET)\
        .blob(f'logs/bronze_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')\
        .upload_from_string("\n".join(lines), content_type="text/plain; charset=utf-8")

def _http_get(params: dict, attempt: int):
    r = requests.get(API_URL, params=params, timeout=30, headers={"User-Agent":"bees-breweries/1.0"})
    if r.status_code in (429, 500, 502, 503, 504):
        backoff = min(60, 2 ** attempt) + 0.1 * attempt
        logging.warning("HTTP %s, retrying in %.1fs...", r.status_code, backoff)
        time.sleep(backoff)
    r.raise_for_status()
    return r

# ============================ ##### BRONZE ##### ============================

def bronze_extract_and_gate(ds, dag_run, ti, **_):
    """
    Baixa TODOS os dados paginados, grava NDJSON por página em GCS e calcula
    um hash do dataset. Se o hash mudou (ou se force==true), retorna True
    (libera as próximas etapas). Caso contrário, retorna False (ShortCircuit).
    """
    run_date = ds  # YYYY-MM-DD
    gcs = storage.Client(); bkt = gcs.bucket(BUCKET_DATA)
    prefix = f"data/bronze/run_date={run_date}"

    # paginação
    page, total = 1, 0
    hasher = hashlib.sha256()
    log = [f"Start Bronze -> {API_URL}", f"GCS prefix: gs://{BUCKET_DATA}/{prefix}"]

    while True:
        params = {"per_page": PER_PAGE, "page": page}
        resp = _http_get(params, attempt=page)
        rows = resp.json()
        if not rows:
            break

        # grava uma página em NDJSON
        ndjson = "\n".join(json.dumps(x, ensure_ascii=False) for x in rows)
        bkt.blob(f"{prefix}/page={page:05d}.jsonl").upload_from_string(
            ndjson, content_type="application/x-ndjson"
        )

        # hash estável pelo id (ordem-independente)
        for _id in sorted(str(x.get("id", "")) for x in rows):
            hasher.update(_id.encode())

        total += len(rows)
        log.append(f"Saved page {page:05d} ({len(rows)} rows)")
        page += 1
        if len(rows) < PER_PAGE:
            break
        time.sleep(0.2)  # respeita API

    dataset_sha = hasher.hexdigest()
    log.append(f"Total pages: {page-1} | rows: {total} | sha256: {dataset_sha}")

    # grava métricas e o hash do dataset dessa execução
    bkt.blob(f"{prefix}/_metrics.json").upload_from_string(
        json.dumps({"pages": page-1, "rows": total, "sha256": dataset_sha}),
        content_type="application/json"
    )
    bkt.blob(f"{prefix}/_dataset_sha256.txt").upload_from_string(dataset_sha, content_type="text/plain")

    # compara com o último hash processado
    ctl_blob = bkt.blob("data/bronze/last_update.txt")
    last = ctl_blob.download_as_text().strip() if ctl_blob.exists() else ""
    changed = (dataset_sha != last)

    # permite forçar via Trigger DAG → conf {"force": true}
    force = bool((dag_run.conf or {}).get("force"))

    if changed or force:
        ctl_blob.upload_from_string(dataset_sha, content_type="text/plain")
        log.append("Change detected (or forced). Downstream will run.")
        ti.xcom_push(key="bronze_changed", value=True)
        _save_log(log)
        return True
    else:
        log.append("No change detected. Downstream will be skipped.")
        ti.xcom_push(key="bronze_changed", value=False)
        _save_log(log)
        return False


def bronze_load_to_bigquery(ds, **_):
    """
    Carrega o BRONZE (raw) da execução (run_date=ds) para a tabela
    case-abinbev-469918.Medallion.bronze em formato JSONL (WRITE_TRUNCATE).
    """
    client = bigquery.Client(project=PROJECT_ID)
    # garante dataset e tabela
    try:
        client.get_dataset(DATASET)
    except Exception:
        client.create_dataset(bigquery.Dataset(f"{PROJECT_ID}.{DATASET}"))

    client.query(f"""
      CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.bronze` (
        id STRING, name STRING, brewery_type STRING,
        address_1 STRING, address_2 STRING, address_3 STRING,
        city STRING, state_province STRING, postal_code STRING, country STRING,
        longitude STRING, latitude STRING, phone STRING, website_url STRING,
        state STRING, street STRING
      )
    """).result()

    uri = f"gs://{BUCKET_DATA}/data/bronze/run_date={ds}/page=*.jsonl"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_uri(
        uri, f"{PROJECT_ID}.{DATASET}.bronze", job_config=job_config
    ).result()

# ========================== ##### /BRONZE ##### ============================

default_args = dict(
    owner="data-eng",
    retries=2,
    retry_delay=timedelta(minutes=5),
    email_on_failure=True,
)

with DAG(
    dag_id="bees_breweries_daily",
    description="Open Brewery DB → Medallion (Bronze/Silver/Gold) — Parte 1: Bronze",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["bees","medallion","bronze"],
) as dag:

    # ##### BRONZE ########################################################
    extract_and_gate = ShortCircuitOperator(
        task_id="bronze_extract_and_gate",
        python_callable=bronze_extract_and_gate,
    )

    load_bronze = PythonOperator(
        task_id="bronze_load_to_bigquery",
        python_callable=bronze_load_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )

    extract_and_gate >> load_bronze

    # ##### SILVER ########################################################
# ============================ ##### SILVER ##### ============================

def silver_transform_and_save_parquet(ds, **_):
    """
    Transforma o BRONZE em SILVER:
    - normaliza tipos (longitude/latitude -> FLOAT64)
    - padrões de limpeza (trim/LOWER onde útil)
    - gera 'state_partition' (0..50) para particionamento
    - grava tabela particionada/clusterizada no BigQuery
    - exporta uma cópia em Parquet para o GCS (silver)
    """
    bq = bigquery.Client(project=PROJECT_ID)

    # 1) Cria/atualiza a tabela particionada/clusterizada direto via SQL
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

    # 2) Exporta uma cópia em Parquet para o GCS (colunar)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.silver`"
    df = bq.query(query).result().to_dataframe(create_bqstorage_client=True)
    path_parquet = f"gs://{BUCKET_DATA}/data/silver/breweries_transformed/breweries_transformed.parquet"
    df.to_parquet(path_parquet, index=False)  # gcsfs/pyarrow cuidam do GCS

    _save_log([
        "Silver: tabela particionada/clusterizada criada/atualizada.",
        f"Silver Parquet salvo em: {path_parquet}",
        f"Linhas no Silver: {len(df)}",
    ])

# ---------------------------------------------------------------------------

silver_build = PythonOperator(
    task_id="silver_transform_and_save_parquet",
    python_callable=silver_transform_and_save_parquet,
    op_kwargs={"ds": "{{ ds }}"},
)

# encadeamento (o Bronze já tem o ShortCircuit na frente)
load_bronze >> silver_build

# ========================== ##### /SILVER ##### ============================
    

# ##### GOLD ##########################################################
# ============================ ##### GOLD ##### ============================

def gold_aggregate_and_save(ds, **_):
    """
    Agrega o SILVER em GOLD:
    - COUNT(*) por country/state/brewery_type
    - tabela clusterizada no BigQuery
    - exporta Parquet no GCS (opcional)
    """
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

    # Exporta uma cópia colunar (útil pra debug/consumo externo)
    df = bq.query(f"SELECT * FROM `{PROJECT_ID}.{DATASET}.gold`") \
           .result().to_dataframe(create_bqstorage_client=True)
    path_parquet = f"gs://{BUCKET_DATA}/data/gold/breweries_aggregated.parquet"
    df.to_parquet(path_parquet, index=False)

    _save_log([
        "Gold: agregado criado/atualizado.",
        f"Gold Parquet salvo em: {path_parquet}",
        f"Linhas no Gold: {len(df)}",
    ])

gold_build = PythonOperator(
    task_id="gold_aggregate_and_save",
    python_callable=gold_aggregate_and_save,
    op_kwargs={"ds": "{{ ds }}"},
)

# encadeamento final
silver_build >> gold_build

# ========================== ##### /GOLD ##### ============================

