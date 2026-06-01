# AWS Batch Data Pipeline

A batch data pipeline built on **PySpark** and **AWS EMR Serverless**, implementing a Medallion Architecture (Bronze → Silver → Gold) for processing financial transaction data. The Gold layer is served to Amazon Redshift for BI and analytics.

---

## Architecture Overview

```
[Raw Sources]          [S3 Data Lake]                          [Warehouse]
CSV / JSON   ──────►  Bronze  ──►  Silver  ──►  Gold  ──────►  Amazon Redshift
                      (raw)      (cleaned)   (star schema)      (BI / ML)
                                     │
                                 Quarantine
                              (bad/duplicate records)
```

### Layer Descriptions

| Layer | Format | Description |
|---|---|---|
| **Bronze** | CSV / JSON | Raw ingested files, immutable |
| **Silver** | Parquet | Cleaned, typed, audited; bad records quarantined |
| **Gold** | Parquet | Star schema — dimensions + facts ready for reporting |
| **Quarantine** | Parquet | Records rejected during mandatory-column or dedup audits |

---

## Datasets

| Dataset | Source File | Write Mode | Key Notes |
|---|---|---|---|
| `transactions` | `transactions_data.csv` | **append** | Immutable events; dedup handled within batch |
| `cards` | `cards_data.csv` | **upsert** | Mutable; credit limit, expiry can change (SCD) |
| `users` | `users_data.csv` | **upsert** | Mutable; address, income can change (SCD) |
| `mcc` | `mcc_codes.json` | **overwrite** | Reference table; replaced in full each run |

---

## Project Structure

```
AWS-batch-data-pipeline/
├── notebooks/
│   └── *.ipynb                     # Jupyter notebooks for exploration and development
├── jobs/
│   └── bronze_to_silver_job.py     # PySpark entry point (EMR / local)
├── src/aws_pipeline/
│   ├── schemas/                    # PySpark StructType definitions for Spark runtime
│   │   ├── silver_schema.py        # PySpark StructType definitions for Silver layer
│   │   └── gold_schema.py          # PySpark StructType definitions for Gold layer
│   └── transformation/             # Silver transformation folder
│   │    ├── load_data_source.py     # Bronze readers (CSV, JSON)
│   │    ├── data_transform.py       # Column-level cleaning functions
│   │    └── bronze_to_silver.py     # Audit, transform, write orchestration
│   └── aggregation/                # Gold transformation folder
│       ├── ...
├── terraform/
│   ├── environments/dev/           # Dev environment S3 buckets
│   └── modules/storage/            # Reusable S3 module
├── warehouse/
│   └── redshift_ddl_fact_tables.sql  # Redshift DDL (applied manually)
├── scripts/
│   ├── run_job_dev.sh              # Submit job to AWS EMR Serverless
│   └── run_job_dev_local.sh        # Submit job to local Docker Spark cluster
├── sample_data/bronze/             # Local sample data for development
├── docker-compose.yml              # Local Spark cluster (1 master + 2 workers)
└── pyproject.toml
```

---

## Getting Started

### Prerequisites

- Python >= 3.11
- Docker & Docker Compose (for local runs)
- AWS CLI configured (for EMR runs)
- Terraform (for infrastructure provisioning)

### Clone the repository

```bash
# Please do not clone this repository yet. It's currently a mess of experiments and unfinished code. Check back later!
```

### Install the Package

```bash

```

## Running the Pipeline

### Local (Docker)

1. Build the wheel:
   ```bash
   pip wheel . -w dist/
   ```

2. Start the Spark cluster:
   ```bash
   docker-compose up -d
   ```

3. Submit the job:
   ```bash
   bash scripts/run_job_dev_local.sh
   ```

   The script connects to the `spark-master` container and runs `bronze_to_silver_job.py` against the `sample_data/` directory.

   | Port | Service |
   |---|---|
   | `9090` | Spark Master Web UI |
   | `4040` | Spark Application Web UI |
   | `8081` | Worker 1 UI |
   | `8082` | Worker 2 UI |

### AWS EMR Serverless

```bash
bash scripts/run_job_dev.sh
```

You will be prompted for:
- S3 URI for input/output/quarantine paths
- S3 URI for the PySpark script and `.whl` package
- EMR Application ID and Execution Role ARN
- Job name and S3 log path

To run a single dataset (e.g., from Airflow), set the `DATASET` environment variable:

```
This feature is not yet implemented.
```

If `DATASET` is unset, all datasets are processed sequentially.

---

## Infrastructure (Terraform)

```bash
cd terraform/environments/dev
terraform init
terraform apply
```

This provisions two S3 buckets:
- **Data lake bucket** — folders: `bronze/`, `silver/`, `gold/`
- **Code bucket** — folders: `etl/`, `dags/`

---

## Silver Layer — Processing Pattern

Each dataset follows an **Audit → Transform → Audit → Write** pattern:

```
Read Bronze
    │
    ▼
audit_mandatory_columns  ──► quarantine (null mandatory fields)
    │
    ▼
transform (clean, cast, rename)
    │
    ▼
audit_duplicates (upsert datasets only) ──► quarantine (duplicates)
    │
    ▼
schema_enforcing + add_audit_columns
    │
    ├──► write_quarantine  (append, partitioned by source + date)
    └──► _write_dataset    (append / upsert / overwrite)
```

### Audit Columns (all Silver & Gold tables)

| Column | Description |
|---|---|
| `_created_at` | Timestamp the record was first written (UTC) |
| `_updated_at` | Timestamp of the last write (UTC) |
| `_processing_id` | Spark application ID of the job that wrote the record |
| `_source_file` | S3 path of the source file |
| `_batch_logical_date` | Logical processing date (UTC) |
| `_is_deleted` | Soft-delete flag |

---

## Gold Layer — Star Schema

The Gold layer models data as a star schema for Amazon Redshift:

**Dimensions:** `Date`, `Time`, `Customer` (SCD), `Account` (SCD), `Merchant`, `Location`, `TransactionType`, `TransErrorType`, `Currency`

**Facts:**
- `AccountTransactionFact` — one row per transaction
- `AccountMonthlySnapshotFact` — monthly aggregated snapshot per account
- `TemporalTrendFact` — time-based trend aggregations per merchant
- `AccountOwnerFactless` — factless fact tracking account-customer ownership

> Redshift DDL scripts are in [`warehouse/`](warehouse/) and applied **manually** following a change-control process. See [`warehouse/README.md`](warehouse/README.md) for guidelines.

---

## Data Governance

- **Timezone:** All `Timestamp` fields stored in UTC. Conversion to local time is handled at the BI layer.
- **Surrogate Keys:** MD5 hashing for distributed scalability on Spark.
- **PII / Sensitive fields:** Tagged in schema definitions. Access restricted via AWS Lake Formation column-level security.
- **Card numbers:** Masked — only the last 4 digits are stored (`mask_card_number`).
- **Idempotency:** Delegated to the orchestrator (Airflow). Each batch should succeed exactly once; use a sensor on `_processing_id` before re-triggering.

---

## Roadmap

Items are grouped by engineering concern. Each section is independent and can be prioritized separately.

---

### 1. Data Source & Ingestion

Establish a realistic, end-to-end ingestion path to replace the current static sample files.

- [ ] Provision a transactional database (e.g., PostgreSQL / Aurora) and implement a transaction simulator to generate realistic financial event streams
- [ ] Build an ingestion layer that extracts from the source database and lands raw files into the Bronze S3 prefix — supporting both full-load and incremental (CDC) patterns

---

### 2. Open Table Format — Apache Iceberg

Replace the current Parquet + manual upsert strategy with Iceberg across all layers to gain first-class ACID semantics and operational flexibility.

- [ ] Migrate Bronze, Silver, and Gold layers to Iceberg table format
  - [ ] Enable time travel and snapshot isolation for point-in-time recovery
  - [ ] Adopt Iceberg schema evolution to eliminate full partition rewrites on schema changes
  - [ ] Replace the custom `upsert_partition` implementation with Iceberg row-level `MERGE INTO`

---

### 3. Data Quality & Observability

Instrument every layer with automated quality checks so failures are caught at the source, not in production dashboards.

- [ ] Integrate **Great Expectations (GX)** as the data quality framework
  - [ ] Author Expectation Suites per dataset at each layer (Bronze: schema/nulls, Silver: referential integrity/ranges, Gold: aggregation consistency)
  - [ ] Execute validations as a post-write step in each transform job; publish results to GX Data Docs
  - [ ] Route validation failures to alerting (Airflow task failure + SNS notification)

---

### 4. Orchestration — Apache Airflow

Replace the current manual `spark-submit` scripts with a fully managed, observable DAG.

- [ ] Author a Bronze → Silver → Gold DAG in Airflow (MWAA or self-hosted)
  - [ ] Model each dataset (`transactions`, `cards`, `users`, `mcc`) as an independent task group to allow targeted retries without full-pipeline re-runs
  - [ ] Add an upstream sensor that validates `_processing_id` uniqueness before triggering a new batch run, enforcing exactly-once semantics
  - [ ] Embed GX validation as a downstream task after each transform step; gate the next layer on validation success
  - [ ] Parameterize DAG runs with `logical_date` to support backfill and historical reprocessing

---

### 5. Semantic Layer — YAML-driven Metrics (Gold)

Decouple metric definitions from pipeline code so analysts and BI consumers can own their own aggregations without modifying Spark jobs.

- [ ] Design a YAML specification format per domain (e.g., `metrics/transactions.yml`) declaring measures, dimensions, filters, and aggregation granularity
- [ ] Implement a code-generation step that reads YAML definitions and materializes the corresponding aggregated tables or views in the Gold layer
- [ ] Persist a metrics lineage record (source dataset, YAML version, run timestamp) to the audit table on each materialization

---

### 6. Serving & BI

Expose the Gold layer to downstream consumers through a performant, low-maintenance query engine.

- [ ] Configure **Redshift Spectrum** external schemas to query Gold Parquet/Iceberg tables on S3 directly, avoiding full data loads into Redshift managed storage
- [ ] Create **Materialized Views** in Redshift for high-frequency dashboard queries (e.g., daily transaction summary, monthly account snapshot); define a refresh schedule aligned with pipeline SLA
- [ ] Build a **Power BI dataset** connected to Redshift (DirectQuery mode) with dynamic date filtering driven by `date_key` / `_batch_logical_date`, so reports always reflect the latest completed batch without manual refresh

---

### 7. Reliability & Fault Tolerance

Harden the pipeline against transient infrastructure failures and partial-write corruption so that no manual intervention is required for recoverable errors.

- [ ] **Spark job retry strategy** — configure EMR Serverless job retries with exponential backoff and jitter; distinguish retriable failures (spot interruption, throttling) from non-retriable ones (schema mismatch, DQ violation) to avoid wasting capacity on guaranteed-to-fail retries
- [ ] **Idempotent writes** — ensure every transform job can be safely re-run for the same `_processing_id` without producing duplicate records; enforce this as a delivery contract in the chosen validation approach
- [ ] **Partial-write guard** — implement a write-then-rename pattern (write to a `_tmp/` S3 prefix, atomic rename on success) so that a mid-job failure never leaves a partially committed partition visible to downstream readers
- [ ] **Dead-letter queue (DLQ)** — route records that fail schema validation or transformation to a dedicated S3 DLQ prefix, preserving raw payloads for offline investigation and selective replay
- [ ] **Checkpoint & resume for large datasets** — record per-partition completion state to DynamoDB or S3; on restart, skip already-committed partitions rather than reprocessing the entire batch
- [ ] **Airflow retry policy** — define per-task `retries`, `retry_delay`, and `retry_exponential_backoff` settings; set `on_failure_callback` to publish structured alerts (task, run_id, error class) to SNS / PagerDuty
- [ ] **End-to-end pipeline SLA** — define a DAG-level SLA (e.g., Gold layer must be ready by 06:00 UTC); emit an SLA-miss event to the alerting channel if the deadline is breached, even when individual tasks succeed

---

### 8. CI/CD

Automate the full delivery lifecycle — from a passing test suite to a deployed job in production.

- [ ] **Continuous Integration** — run on every pull request targeting `main`:
  - [ ] Code linting and formatting checks (`ruff` / `black`)
  - [ ] Terraform `plan` diff posted as a PR comment for infrastructure changes
- [ ] **Continuous Delivery — application code:**
  - [ ] On merge to `main`, build and publish the `.whl` package to the S3 code bucket (`etl/`)
  - [ ] Trigger an Airflow DAG deployment or update the EMR Serverless job definition automatically
- [ ] **Continuous Delivery — infrastructure:**
  - [ ] Apply `terraform apply` automatically for the `dev` environment; require manual approval gate for `prod`
  - [ ] Manage Redshift DDL schema migrations through a versioned migration tool (e.g., Flyway or a custom migration runner), replacing the current manual apply process
