# Audit-Write-Audit-Publish Framework

This package contains a small, contract-driven implementation of the
Audit-Write-Audit-Publish pattern for data quality enforcement.

The framework is currently used by the Silver to Gold pipeline, but it lives
under `quality/` instead of `aggregation/` because AWAP is a data quality
concern, not a Gold-layer concern. The Bronze to Silver pipeline can adopt the
same primitives later without coupling its refactor to the current Gold work.

## Why AWAP

Gold tables feed BI dashboards and analytical workloads, so the pipeline should
not publish partially joined, stale, duplicated, or schema-drifted data directly
to production paths.

The intended flow is:

```text
Audit input
  -> Write isolated staging output
    -> Audit staged output
      -> Publish only if critical checks pass
```

For the current project, `source -> bronze` remains a raw landing step and does
not need full AWAP. Bronze should preserve raw evidence with lightweight
ingestion checks. `bronze -> silver` should eventually use the same framework,
but that refactor is intentionally deferred.

## Design Goals

- Keep orchestration independent from the execution engine. The local/EMR
  runner can call a dataset end-to-end, while Airflow can later call the build
  and publish steps as separate tasks.
- Avoid per-table copy-paste. Dataset-specific behavior is declared in contracts
  instead of hard-coded into one large function per table.
- Support mixed load strategies. Some dimensions are full refreshes, some facts
  are incremental partition overwrites, and some tables need CDC-style upserts.
- Protect dashboard SLA. Dashboard-facing datasets must run on bounded windows,
  with SLA values loaded from shared quality config.
- Fail closed on critical checks. Warnings are reported but do not block publish.

## Key Files

| File | Responsibility |
|---|---|
| `contracts.py` | Dataset contract dataclasses and load strategy enum |
| `contract_audit.py` | Generic Audit 2 checks for staged datasets |
| `publisher.py` | Publish behavior for full, append, partition overwrite, and CDC upsert |
| `runner.py` | Generic `audit_write_dataset` and `audit_publish_dataset` entry points |
| `common.py` | Shared AWAP helpers: schema checks, critical failure detection, SLA validation |
| `config/quality.yml` | Shared quality/SLA configuration consumed by AWAP and future patterns |

Gold-specific contracts are declared outside this package:

```text
src/aws_pipeline/aggregation/gold_contracts.py
```

## Dataset Contracts

A Gold dataset is described with a `GoldDatasetContract`. The contract defines
what the framework needs to know in order to audit and publish safely:

```python
GoldDatasetContract(
    name="account_transaction_fact",
    schema=AccountTransactionFactSchema,
    staging_path="gold/_staging/account_transaction_fact",
    publish_path="gold/account_transaction_fact",
    load_strategy=GoldLoadStrategy.PARTITION_OVERWRITE,
    source_names=("transactions",),
    dependency_names=(
        "customer_dimension",
        "account_dimension",
        "transaction_type_dimension",
        "merchant_dimension",
        "location_dimension",
        "trans_error_type_dimension",
        "currency_dimension",
    ),
    required_columns=(
        "transaction_id",
        "date_key",
        "time_key",
        "customer_key",
        "account_key",
        "transaction_type_key",
        "merchant_key",
        "merchant_location_key",
        "trans_error_type_key",
        "currency_key",
        "transaction_amount",
    ),
    unique_key=("transaction_id",),
    partition_columns=("date_key",),
    row_count_source="transactions",
)
```

This makes table behavior explicit and reviewable. Adding a new Gold table
should usually mean adding a contract plus a transform builder, not duplicating
the AWAP implementation.

## Load Strategies

The framework supports four strategies:

| Strategy | Use Case | Publish Behavior |
|---|---|---|
| `FULL` | Static or small reference dimensions | Overwrite the published table |
| `INCREMENTAL_APPEND` | Immutable append-only records | Append staging rows |
| `PARTITION_OVERWRITE` | Windowed facts and aggregates | Dynamically overwrite affected partitions |
| `CDC_UPSERT` | Slowly changing or mutable entities | Replace records matching configured CDC keys |

The current implementation uses Parquet paths. This is sufficient for a local
data lake exercise, but true atomic publish semantics are better handled by a
table format such as Iceberg, Delta, or Hudi.

## Audit 1 Checks

Audit 1 runs before writing staging. Its job is to decide whether the input is
safe enough to transform and stage. It should catch upstream readiness issues
early, before the pipeline spends compute on joins or writes partial output.

Typical Audit 1 checks:

| Check | Purpose |
|---|---|
| `source_readable` | Input table/path can be read by Spark |
| `source_not_empty` | Expected batch/window has at least one row |
| `window_sla` | Dashboard datasets use a bounded window within the configured SLA |
| `schema_presence` | Required input columns are present |
| `business_key_not_null` | Natural keys needed downstream are not null |
| `event_time_bounds` | Event timestamps belong to the expected processing window |
| `duplicate_business_key` | Batch/window does not contain duplicate immutable keys |
| `freshness` | Input data is recent enough for the target SLA |
| `quarantine_ratio` | Bad-record ratio stays below an acceptable threshold |

Audit 1 is intentionally source-specific. For example, Silver transactions need
checks on `transaction_id`, `timestamp`, `client_id`, `card_id`, `merchant_id`,
and `amount`, while a static reference table may only need readability,
non-empty, schema, and key uniqueness checks.

For dashboard-facing datasets, Audit 1 must stay bounded. The expected pattern
is to audit only the current processing window, not rescan the full history.

```text
Audit 1 input window
  -> transform/build staged dataset
  -> write staging
```

The current generic framework focuses on Audit 2 because staged-output checks
are more reusable across tables. Audit 1 rules are expected to be added through
dataset-specific pre-write checks as each source contract matures.

## Audit 2 Checks

`contract_audit.py` validates staged output before publishing:

- Schema matches the declared contract schema.
- Staging is not empty.
- Required columns have no nulls.
- Unique key has no duplicates.
- Freshness is within the configured SLA.
- Optional source reconciliation checks compare row count and aggregate sums.
- Publish gate fails if any critical check fails.

The checks are intentionally bounded. For dashboard-facing datasets, expensive
validation must not consume the full refresh budget.

## SLA Configuration

SLA values are loaded through the shared quality config loader:

```yaml
sla:
  dashboard_refresh_minutes: 5
  publish_budget_seconds: 90
```

Default path:

```text
config/quality.yml
```

Override path:

```bash
QUALITY_CONFIG_PATH=/path/to/quality.yml
```

This is deliberately not AWAP-specific. Other quality patterns can reuse the
same SLA policy without depending on AWAP internals.

## Airflow Migration Path

The current project does not require Airflow, so `silver_to_gold.py` still
contains a local/EMR runner. The runner is split into Airflow-friendly task
boundaries:

```text
build_staging_task(dataset)
  -> publish_dataset_from_staging(dataset)
```

The staging task writes an isolated staging path and returns the path as a
string. The publish task reads staging from storage, runs Audit 2, then publishes
according to the dataset contract.

This keeps scheduler concerns out of the transform code. A future Airflow DAG
can orchestrate dependencies between datasets without rewriting the quality
logic.

