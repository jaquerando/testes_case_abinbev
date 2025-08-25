

# Data pipeline summary

This data pipeline ingests data from the Open Brewery DB API, performs transformations, and loads it into a data lake structured with Bronze, Silver, and Gold layers. 
It leverages Apache Airflow for orchestration, ensuring scheduled execution, error handling, and dependency management between the stages. 
Google Cloud Storage is used for data storage, and BigQuery serves as the final destination for analytical queries.

- **GCP Project:** `case-abinbev-469918`
- **BQ Dataset:** `Medallion`
- **BQ Tables:** `bronze`, `silver`, `gold`
- **Composer bucket:** `us-central1-composer-case-165cfec3-bucket`
- **DAG:** `bees_breweries_daily`
- **Schedule:** `0 3 * * *` (daily at 03:00 UTC ≈ 00:00 America/Sao_Paulo)
- **Source API:** `https://api.openbrewerydb.org/v1/breweries` (with pagination)

Medallion Architecture: Implements the Bronze, Silver, Gold layered approach for data lake organization.
Incremental Loading: Only processes new or changed data, improving efficiency.
Data Validation: Uses hash comparisons to ensure data integrity.
Monitoring and Alerting: Includes logging to GCS and email alerts for proactive monitoring.
Orchestration: Utilizes Apache Airflow for scheduling, task dependencies, and error handling.

## End-to-end flow

**Bronze** 
The DAG paginates the API (per_page=200) and iterates until an empty page is returned—so all pages are fetched, not just page 1. The full snapshot is serialized to a single UTF-8 NDJSON and written to GCS, plus a timestamped archive copy.

Change detection & idempotency: A SHA-256 is computed over the NDJSON bytes. The hash is compared to the previous run’s value stored in gs://us-central1-composer-case-165cfec3-bucket/control/bronze_sha256.txt.

If unchanged, Silver/Gold are skipped automatically (incremental gate).

If changed (or forced), the pipeline proceeds to Silver and Gold.

**Silver** 
Reads Bronze from BigQuery, applies text repairs for known mojibake (e.g., Caf�→Café, Stra�e→Straße, K�rnten→Kärnten, Nieder�sterreich→Niederösterreich, W�rthersee→Wörthersee, Wimitzbr�u→Wimitzbräu), normalizes Unicode (NFC), and casts longitude/latitude to FLOAT64. It derives a deterministic state_partition integer and writes Parquet to GCS, then loads Medallion.silver (range-partitioned by state_partition and clustered by country, city).

**Gold** 
Aggregates Silver to counts per country, state, brewery_type, writes Parquet to GCS, and loads Medallion.gold (clustered by country, state, brewery_type).

## Storage & control artifacts (GCS)

Bronze NDJSON (current): gs://us-central1-composer-case-165cfec3-bucket/data/bronze/breweries.ndjson

Bronze NDJSON (archive): .../data/bronze/archive/breweries_<timestamp>.ndjson

Silver Parquet: .../data/silver/breweries_transformed.parquet + archive

Gold Parquet: .../data/gold/breweries_aggregated.parquet + archive

Control file (hash): gs://us-central1-composer-case-165cfec3-bucket/control/bronze_sha256.txt

Per-run logs: .../logs/<timestamp>/{bronze|silver|gold}.log

## Scheduling & forcing

Schedule: 0 3 * * * (daily at 03:00 UTC, ≈00:00 America/Sao_Paulo). catchup=false.

Force a full run (bypass the hash gate) via:

Airflow Run Config: {"force": true}, or

Airflow Variable: bees_force=true (then trigger normally).

## Operational characteristics

Incremental loading: downstream stages run only when data changes, saving cost and time.

### Reliability: 
API calls use timeouts; Airflow handles retries and task-level error handling.

### Observability: 
Each stage writes a concise log to GCS with page/row counts, hash values, and outcomes (“changed”/“no change”)

**Airflow / Composer**

DAG view shows task status and Gantt.

Task logs are also copied to gs://.../logs/YYYYMMDD/*.log for quick download.

Email alerts can be enabled on task failure (SMTP/SendGrid or Google Workspace).

**Cloud Run Jobs**

Job execution logs live in Cloud Logging (resource.type="run_job").

Use Log-based Metrics → Alerting Policies to notify on failures or long runtimes.

**Data quality (Dataplex)**

You can run Data Profile & Data Quality scans on Medallion.silver and Medallion.gold.

Recommended rules:

Silver: id NON_NULL & UNIQUE, latitude/longitude ranges, value set for brewery_type.

Gold: reconciliation vs Silver (total_breweries sanity check).

Results surface in BigQuery table tabs (Profile/Quality) and in Dataplex.

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

**Bronze**

Schema mirrors the API payload (STRING fields for raw text):
id, name, brewery_type, address_1, address_2, address_3, city, state_province, postal_code, country, longitude, latitude, phone, website_url, state, street.

**Silver** 

Clean/Normalize → Parquet → BigQuery

Reads Bronze from BigQuery and applies text repairs to fix known mojibake issues from the source (e.g., Caf�→Café, Stra�e→Straße, K�rnten→Kärnten, Nieder�sterreich→Niederösterreich, W�rthersee→Wörthersee, Wimitzbr�u→Wimitzbräu).
These are hard-coded in the DAG so the analytical layer is clean even if the API returns broken characters.

Normalizes Unicode to NFC, casts longitude/latitude to FLOAT64.

Creates state_partition (deterministic integer) for range partitioning.

**Gold**

Aggregation and final summary of info

# DDL

## DATASET

CREATE SCHEMA IF NOT EXISTS `case-abinbev-469918.Medallion`;

### BRONZE
```sql
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
```

### SILVER
```sql
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
```
### GOLD
```sql
CREATE TABLE IF NOT EXISTS `case-abinbev-469918.Medallion.gold` (
  country         STRING,  -- Country name (from Silver)
  state           STRING,  -- State/Province (from Silver)
  brewery_type    STRING,  -- Type (e.g., micro, brewpub, large, bar, etc.)
  total_breweries INT64    -- Aggregated count
)
CLUSTER BY country, state, brewery_type;
```

**Columns**

- country (STRING): Country of the brewery.

- state (STRING): State/Province (already normalized in Silver).

- brewery_type (STRING): Category from the source API (e.g., micro, brewpub, large, bar).

- total_breweries (INT64): Count of rows per (country, state, brewery_type).

**Populated by**

-- Conceptual aggregation used by the pipeline:
```sql
SELECT
  country,
  state,
  brewery_type,
  COUNT(*) AS total_breweries
FROM `case-abinbev-469918.Medallion.silver`
GROUP BY 1,2,3;
```

### Gold Sample
```sql
SELECT country, state, brewery_type, total_breweries
FROM `case-abinbev-469918.Medallion.gold`
ORDER BY country, state, brewery_type;
```

# How to deploy & run

## Service Account
Every call to a Google Cloud API is made by an identity. For human users that’s Gmail/Workspace account; for workloads (Airflow tasks, Cloud Run jobs, build steps) it’s a Service Account (SA)—a robot identity that can be granted roles.

**Principle of Least Privilege (PLP)**

Give each workload only the permissions it needs—no more. This reduces blast radius if keys are leaked or code goes wrong, and it makes audits clearer.

We use a single SA at first (to reduce moving parts), and then show how to tighten it. You can split into multiple SAs later (e.g., one for Composer, one for Cloud Run Jobs).

**PIs & IAM (one-time)**

```bash
gcloud services enable \
  artifactregistry.googleapis.com run.googleapis.com cloudscheduler.googleapis.com \
  cloudbuild.googleapis.com composer.googleapis.com bigquery.googleapis.com \
  storage.googleapis.com pubsub.googleapis.com billingbudgets.googleapis.com \
  monitoring.googleapis.com logging.googleapis.com --project $PROJECT_ID
```

Grant the Compute Default SA (or a dedicated SA) the minimal roles (can be tightened later):

```bash
SA="$PROJECT_ID-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/storage.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/run.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/cloudscheduler.admin"

```

**What this is doing:**

| Component                      | What it does                                                     | Minimal roles (reason)                                                                                                                                                                                   |
| ------------------------------ | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Composer / Airflow workers** | Read/write files in GCS, create/query BigQuery tables, emit logs | `roles/storage.objectAdmin` (write artifacts/logs under the Composer bucket), `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` (create tables, run queries), `roles/logging.logWriter` (send logs) |
| **Cloud Run Job**              | Runs the containerized ETL outside Airflow                       | Same set as above if it touches the same data; additionally `roles/artifactregistry.reader` (pull image)                                                                                                 |
| **Cloud Scheduler**            | Triggers Cloud Run Job on a schedule                             | `roles/run.invoker` (invoke the job’s run endpoint)                                                                                                                                                      |
| **Cloud Build**                | Builds the container image from the Dockerfile                  | `roles/artifactregistry.writer` (push images), `roles/storage.readOnly` (read build context if in GCS)                                                                                                   |
| **Budgets/Alerts**             | Publishes spend alerts                                           | Billing UI auto-configures; if using Pub/Sub, that topic needs a publisher binding for the billing service agent                                                                                         |


## Creating the Composer environment
```bash
gcloud composer environments create $COMPOSER_ENV \
  --project $PROJECT_ID \
  --location $REGION \
  --image-version=composer-3-airflow-2.8.1 \
  --service-account "$PROJECT_ID-compute@developer.gserviceaccount.com"
```

**What this is doing:**

--project ties the environment to the billing/project context.

--location places the GKE Autopilot cluster and Composer bucket in us-central1 (keeping latency and egress predictable for BigQuery + Run).

--image-version pins a Composer 3 image that bundles Airflow 2.8.x—a stable, long-supported release with good provider compatibility.

--service-account picks the identity Airflow workers use to access GCS/BigQuery/etc. (the roles we granted above apply to this identity).

## Install PyPI packages (either via UI → Packages PyPI or CLI):

Airflow runs tasks in the Composer image; if the DAG imports libraries (pandas, pyarrow, GCS/BigQuery SDKs) they must exist on the workers. You can install through the UI (Packages PyPI) or via CLI:

### exact pins that worked in this project
```bash
gcloud beta composer environments update $COMPOSER_ENV \
  --location $REGION \
  --update-pypi-package=google-cloud-storage==2.16.0 \
  --update-pypi-package=google-cloud-bigquery==3.25.0 \
  --update-pypi-package=pandas==2.2.2 \
  --update-pypi-package=pyarrow==16.1.0 \
  --update-pypi-package=gcsfs==2024.6.1 \
  --update-pypi-package=requests==2.32.3
```

### Airflow Variables (UI → Admin → Variables):

| Key              | Value                                        |
| ---------------- | -------------------------------------------- |
| `BQ_PROJECT`     | `case-abinbev-469918`                        |
| `BQ_DATASET`     | `Medallion`                                  |
| `GCS_BUCKET`     | `us-central1-composer-case-165cfec3-bucket`  |
| `BRONZE_PREFIX`  | `data/bronze`                                |
| `SILVER_PREFIX`  | `data/silver`                                |
| `GOLD_PREFIX`    | `data/gold`                                  |
| `CONTROL_PREFIX` | `control`                                    |
| `LOGS_PREFIX`    | `logs`                                       |
| `API_BASE`       | `https://api.openbrewerydb.org/v1/breweries` |
| `PAGE_SIZE`      | `200`                                        |
| `DAG_SCHEDULE`   | `0 3 * * *` (03:00 UTC)                      |


**Trigger manually (Cloud Shell)**
```bash
gcloud composer environments run composer-case --location=us-central1 \
  dags trigger -- bees_breweries_daily --conf='{"force": true}'
```

Force full rebuild (alternative)

Force reprocess: trigger the DAG with Config: {"force": true} to override the bronze hash gate and run the full Bronze→Silver→Gold chain.
Go to Airflow → Admin → Variables set {"force": true} and trigger normally.

### Deploying the DAG

**Upload the DAG**

- Put `dag.py` in: `gs://us-central1-composer-case-165cfec3-bucket/dags/dag.py`

- Upload dag.py to gs://us-central1-composer-case-165cfec3-bucket/dags/.
Composer syncs it automatically; within ~60s the DAG bees_breweries_daily appears.

Schedule: daily at 03:00 UTC (configurable via variable DAG_SCHEDULE).

Manual run:

Airflow UI → Trigger DAG.

To rebuild all layers now: Run Config → {"force": true} (overrides the bronze hash check).

## Configuring the Google Cloud Storage buckets (layout)

```bash
gs://us-central1-composer-case-165cfec3-bucket/
├─ dags/
│   └─ dag.py
├─ data/
│   ├─ bronze/
│   │   ├─ breweries.ndjson
│   │   └─ archive/breweries_YYYYMMDD_HHMMSS.ndjson
│   ├─ silver/
│   │   ├─ breweries_transformed.parquet
│   │   └─ archive/breweries_transformed_YYYYMMDD_HHMMSS.parquet
│   └─ gold/
│       ├─ breweries_aggregated.parquet
│       └─ archive/breweries_aggregated_YYYYMMDD_HHMMSS.parquet
├─ control/
│   └─ bronze_sha256.txt
└─ logs/
    └─ YYYYMMDD/bronze.log  silver.log  gold.log
```

# 🐳 Containerizing the ETL (Cloud Run Jobs)

This gives you a CLI-style runner outside Airflow (useful for ad-hoc runs or CI).

Containerization packages the code + dependencies into an image that runs identically everywhere: locally, in Cloud Run Jobs, or from a CI pipeline. This gives you:

- Reproducibility (no “works on my machine”).

- Fast startup with prebuilt dependencies.

- Cost efficiency in Run Jobs (scale to zero; pay only while executing).

- Separation of concerns: Airflow orchestrates; the container does the heavy lifting.

## Dockerfile (project root)
A Dockerfile is a build recipe. gcloud builds submit reads it to produce an image, which we store in Artifact Registry; later, Cloud Run Jobs pull that image and execute it.

```bash
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ /app/etl/
# entry point can run the same logic as the DAG tasks (bronze->silver->gold)
ENTRYPOINT ["python", "-m", "etl.main"]

```
**requirements.txt** contains the exact libs the code imports (same pins you used in Composer to keep parity).
It is used: 
- During build (Cloud Build) to create the image.

- At runtime (Cloud Run Jobs) to know how to start the process (ENTRYPOINT).

requirements.txt
```bash
google-cloud-storage==2.16.0
google-cloud-bigquery==3.25.0
pandas==2.2.2
pyarrow==16.1.0
gcsfs==2024.6.1
requests==2.32.3
```

## Build & push (Artifact Registry)
Let's think of Artifact Registry as a private Docker Hub on GCP. 
It stores versioned container images close to the runtime region (lower latency, lower egress), with IAM-controlled access.

**Step 1**: Creates a registry named, e.g., containers in us-central1.

**Step 2**: Configures local Docker/Cloud Build to push/pull from us-central1-docker.pkg.dev.

**Step 3**: Cloud Build builds the image using the Dockerfile, then pushes it to the registry. The result is an immutable artifact identified by $IMAGE_TAG.

```bash
gcloud artifacts repositories create $AR_REPO --repository-format=docker \
  --location=$REGION --description="Breweries containers" || true

gcloud auth configure-docker $REGION-docker.pkg.dev

gcloud builds submit \
  --tag $REGION-docker.pkg.dev/$PROJECT_ID/$AR_REPO/$IMAGE_NAME:$IMAGE_TAG
```


## Create the Cloud Run Job

```bash
gcloud beta run jobs create $RUN_JOB \
  --image $REGION-docker.pkg.dev/$PROJECT_ID/$AR_REPO/$IMAGE_NAME:$IMAGE_TAG \
  --region $REGION \
  --tasks 1 \
  --service-account "$PROJECT_ID-compute@developer.gserviceaccount.com" \
  --set-env-vars PROJECT_ID=$PROJECT_ID,BQ_DATASET=$DATASET,GCS_BUCKET=$COMPOSER_BUCKET
```

**What this is doing:**

--image: which container to run—the one we just built.

--tasks 1: a single parallel task (you can fan out if the code supports sharding).

--service-account: the identity the job uses to access GCS/BigQuery. Grant it only what it needs.

--set-env-vars: pass configuration into the container so the same image can point at different projects/buckets without rebuilding.

**Run on demand**
```bash
gcloud beta run jobs execute $RUN_JOB --region $REGION
```

**Scheduling the Cloud Run Job (Cloud Scheduler)**

Scheduler hits the Cloud Run Jobs API endpoint
```bash 
gcloud scheduler jobs create http breweries-etl-daily \
  --location=$REGION \
  --schedule="0 3 * * *" \
  --uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/$RUN_JOB:run" \
  --http-method=POST \
  --oauth-service-account-email="$PROJECT_ID-compute@developer.gserviceaccount.com" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
```


Now I have two orchestrators:

Airflow (Composer) for the main DAG.

Cloud Run Job + Scheduler for an alternative/backup runner.

I can use one or both as needed.



# Validation queries

**Row counts**
```sql
SELECT 'bronze' AS tbl, COUNT(*) FROM `case-abinbev-469918.Medallion.bronze`
UNION ALL SELECT 'silver', COUNT(*) FROM `case-abinbev-469918.Medallion.silver`
UNION ALL SELECT 'gold',   COUNT(*) FROM `case-abinbev-469918.Medallion.gold`;
```

Writes Parquet to GCS and loads into Silver (WRITE_TRUNCATE).

# Sum up

- Cloud Run Jobs give us a containerized, stand-alone runner for the same ETL logic—perfect for ad-hoc runs, CI/CD smoke tests, or running when Composer is paused. Jobs scale to zero and bill only while running. They’re cheaper and simpler than keeping a VM, and simpler than spinning a transient Dataproc cluster for a small Python ETL.

- Service Accounts are robot identities. Everything in this stack runs as an SA.
Grant only what each piece needs (PLP). Start broad, then tighten.

- Composer is the conductor (orchestrator).
The DAG is the score (steps & dependencies).
Cloud Run Jobs are a portable instrument that can play the same tune without the orchestra—cheaply and on demand.

- Artifact Registry is the instrument: where the images live.
- Cloud Build constructs the instrument from the Dockerfile.
- GCS stores artifacts and logs; 
- BigQuery holds analytical tables.
- Partition/cluster to lower cost. Normalize text to make our lives easy.
- Budgets & Alerts ensure we don’t overspend;
- Cloud Monitoring/Logging tells us when anything goes sideways.










# Looker
**Gold layer — Breweries by country (Looker Studio map)**

![GOLD LOOKER](https://github.com/user-attachments/assets/9571e1af-7774-4aee-8189-06069e1f99bc)

This chart is built from the Gold table (case-abinbev-469918.Medallion.gold). Each bubble represents a country; the bubble size encodes the number of breweries in that country.

Hover a bubble to see the exact value (from the total_breweries metric).

Click a bubble to cross-filter other charts on the page (if enabled).

Gold is the curated, aggregated layer. Here we pre-compute the count of breweries per (country, state, brewery_type) in Silver and then roll it up for country-level views. That keeps the dashboard fast and inexpensive while ensuring the data is already cleaned (Unicode fixes, typed fields) and deduplicated upstream.

- How the metric is calculated

Source: Medallion.gold

Metric: SUM(total_breweries) grouped by country

Update cadence: daily at 03:00 UTC (or on forced runs)

- How to read it

Larger bubble = more breweries in that country.

Countries with no data won’t display a bubble.

Numbers reflect the latest successful pipeline run; they will change when the upstream API changes.

- Considerations

Counts are based on the Open Brewery DB and may include small bars/brewpubs depending on brewery_type.

This view is country-level; use the table or a drill-down page to analyze by state or brewery_type.

















💡 ## **SQL Code:**
```sql
SELECT d.department_name AS department,


```


##  📊 Output:

## 1️⃣
## 2️⃣
## 3️⃣ 

## 📁 3. Project Structure
```css
data_challenge/
│
├── app/
│   ├── __init__.py
│   ├── main.py
│   └── upload.py
│
├── data_challenge.db
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```


### ✅ 1. Setting Up the Environment

### 2. 🐳 Dockerizing the Application ##

1. **Clone the Repository:**
    ```powershell
    git clone <repository_url>
    cd data_challenge
    ```

### 1. Python 3.11+
- [Download Python](https://www.python.org/downloads/)
- Verify installation:
    ```powershell
    python --version
    ```
