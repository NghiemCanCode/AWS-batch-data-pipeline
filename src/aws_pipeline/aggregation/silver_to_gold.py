"""
Gold-layer orchestration entry points.

AWAP implementation details live under ``aws_pipeline.quality.awap`` so this
module stays small as more Gold tables are added.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from .dim_processing.account_dim import process_account_dim
from .dim_processing.currency_dim import generate_currency_dim
from .dim_processing.customer_dim import process_customer_dim
from .dim_processing.date_dim import generate_date_dim
from .dim_processing.location_dim import process_location_dim
from .dim_processing.merchant_dim import process_merchant_dim
from .dim_processing.time_dim import generate_time_dim
from .dim_processing.transaction_type_dim import process_transaction_type_dim
from .dim_processing.trans_error_type_dim import process_trans_error_type_dim
from ..quality.awap_deprecated.common import (
    DASHBOARD_REFRESH_SLA_MINUTES,
    read_table_or_path,
    validate_window_sla,
)
from ..quality.awap_deprecated.contracts import (
    GoldDatasetContract,
    GoldRunContext,
)
from ..quality.awap_deprecated.publisher import (
    configure_gold_iceberg_catalog,
    published_table_identifier,
    read_staging_dataset,
    staging_table_identifier,
)
from ..quality.awap_deprecated.runner import audit_publish_dataset, audit_write_dataset
from ..quality.audit_log import AuditLogContext, quality_audit_log_path
from .fact_processing.account_monthly_snapshot_fact import (
    process_account_monthly_snapshot_fact,
)
from .fact_processing.account_owner_factless import process_account_owner_factless
from .fact_processing.account_transaction_fact import process_account_transaction_fact
from .fact_processing.temporal_trend_fact import process_temporal_trend_fact
from .gold_contracts import GOLD_CONTRACTS


logger = logging.getLogger(__name__)

DATASETS = GOLD_CONTRACTS

PIPELINE_ORDER = (
    "date_dimension",
    "time_dimension",
    "customer_dimension",
    "account_dimension",
    "merchant_dimension",
    "location_dimension",
    "transaction_type_dimension",
    "trans_error_type_dimension",
    "currency_dimension",
    "account_transaction_fact",
    "account_monthly_snapshot_fact",
    "temporal_trend_fact",
    "account_owner_factless",
)

DASHBOARD_DATASETS = {
    dataset_name
    for dataset_name, contract in DATASETS.items()
    if contract.requires_dashboard_window
}

TransformBuilder = Callable[
    [SparkSession, dict[str, DataFrame], dict[str, DataFrame], str, dict],
    DataFrame,
]


def _source_path(input_base_path: str, source_name: str) -> str:
    return f"{input_base_path.rstrip('/')}/silver/{source_name}"


def _gold_path(output_base_path: str, contract: GoldDatasetContract) -> str:
    return published_table_identifier(contract)


def _staging_path(output_base_path: str, contract: GoldDatasetContract) -> str:
    return staging_table_identifier(contract)


def _read_sources(
    spark: SparkSession,
    contract: GoldDatasetContract,
    input_base_path: str,
    options: dict,
) -> dict[str, DataFrame]:
    sources = {
        source_name: read_table_or_path(spark, _source_path(input_base_path, source_name))
        for source_name in contract.source_names
    }

    if (
        "transactions" in sources
        and _is_windowed_run(contract.name, options)
    ):
        sources["transactions"] = _filter_transaction_window(
            sources["transactions"],
            options["window_start"],
            options["window_end"],
        )

    return sources


def _read_dependencies(
    spark: SparkSession,
    contract: GoldDatasetContract,
    output_base_path: str,
    gold_cache: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    dependencies = {}
    for dependency_name in contract.dependency_names:
        if dependency_name in gold_cache:
            dependencies[dependency_name] = gold_cache[dependency_name]
            continue

        dependency_contract = DATASETS[dependency_name]
        configure_gold_iceberg_catalog(spark, output_base_path)
        dependencies[dependency_name] = spark.table(
            _gold_path(output_base_path, dependency_contract)
        )

    return dependencies


def _filter_transaction_window(
    transactions_df: DataFrame,
    window_start: str,
    window_end: str,
) -> DataFrame:
    from pyspark.sql import functions as F

    start = F.lit(window_start).cast("timestamp_ntz")
    end = F.lit(window_end).cast("timestamp_ntz")
    return transactions_df.where(
        (F.col("timestamp") >= start) & (F.col("timestamp") < end)
    )


def _is_windowed_run(dataset_name: str, options: dict) -> bool:
    return (
        dataset_name in DASHBOARD_DATASETS
        and bool(options.get("window_start"))
        and bool(options.get("window_end"))
    )


def _enforce_dashboard_sla(dataset_name: str, options: dict) -> None:
    if dataset_name not in DASHBOARD_DATASETS:
        return

    window_start = options.get("window_start")
    window_end = options.get("window_end")
    if not window_start or not window_end:
        raise ValueError(
            f"{dataset_name} requires window_start/window_end to preserve "
            f"the {DASHBOARD_REFRESH_SLA_MINUTES}-minute dashboard SLA."
        )

    window_ok, window_duration = validate_window_sla(window_start, window_end)
    if not window_ok:
        raise ValueError(
            f"{dataset_name} window violates dashboard SLA: {window_duration}"
        )


def _infer_date_range(transactions_df: DataFrame) -> tuple[str, str]:
    from pyspark.sql import functions as F

    row = transactions_df.agg(
        F.min(F.to_date("timestamp")).alias("start_date"),
        F.max(F.to_date("timestamp")).alias("end_date"),
    ).first()

    if row["start_date"] is None or row["end_date"] is None:
        raise ValueError("Cannot infer date dimension range from empty transactions.")

    return row["start_date"].isoformat(), row["end_date"].isoformat()


def _build_date_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    date_start = options.get("date_start")
    date_end = options.get("date_end")
    if not date_start or not date_end:
        date_start, date_end = _infer_date_range(sources["transactions"])

    return generate_date_dim(date_start, date_end, spark, batch_logical_date)


def _build_time_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return generate_time_dim(spark, batch_logical_date)


def _build_customer_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_customer_dim(sources["users"], batch_logical_date)


def _build_account_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_account_dim(sources["cards"], batch_logical_date)


def _build_merchant_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_merchant_dim(sources["mcc"], batch_logical_date)


def _build_location_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_location_dim(
        batch_logical_date,
        sources["users"],
        sources["transactions"],
    )


def _build_transaction_type_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_transaction_type_dim(sources["transactions"], batch_logical_date)


def _build_trans_error_type_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_trans_error_type_dim(sources["transactions"], batch_logical_date)


def _build_currency_dimension(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return generate_currency_dim(
        spark,
        batch_logical_date,
        currencies=options.get("currencies"),
    )


def _build_account_transaction_fact(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_account_transaction_fact(
        transactions_df=sources["transactions"],
        customer_dim_df=dependencies["customer_dimension"],
        account_dim_df=dependencies["account_dimension"],
        transaction_type_dim_df=dependencies["transaction_type_dimension"],
        merchant_dim_df=dependencies["merchant_dimension"],
        location_dim_df=dependencies["location_dimension"],
        trans_error_type_dim_df=dependencies["trans_error_type_dimension"],
        currency_dim_df=dependencies["currency_dimension"],
        batch_logical_date=batch_logical_date,
    )


def _build_account_monthly_snapshot_fact(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_account_monthly_snapshot_fact(
        dependencies["account_transaction_fact"],
        batch_logical_date,
    )


def _build_temporal_trend_fact(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_temporal_trend_fact(
        dependencies["account_transaction_fact"],
        batch_logical_date,
    )


def _build_account_owner_factless(
    spark: SparkSession,
    sources: dict[str, DataFrame],
    dependencies: dict[str, DataFrame],
    batch_logical_date: str,
    options: dict,
) -> DataFrame:
    return process_account_owner_factless(
        customer_dim_df=dependencies["customer_dimension"],
        account_dim_df=dependencies["account_dimension"],
        batch_logical_date=batch_logical_date,
        account_owner_source_df=sources["cards"],
    )


BUILDERS: dict[str, TransformBuilder] = {
    "date_dimension": _build_date_dimension,
    "time_dimension": _build_time_dimension,
    "customer_dimension": _build_customer_dimension,
    "account_dimension": _build_account_dimension,
    "merchant_dimension": _build_merchant_dimension,
    "location_dimension": _build_location_dimension,
    "transaction_type_dimension": _build_transaction_type_dimension,
    "trans_error_type_dimension": _build_trans_error_type_dimension,
    "currency_dimension": _build_currency_dimension,
    "account_transaction_fact": _build_account_transaction_fact,
    "account_monthly_snapshot_fact": _build_account_monthly_snapshot_fact,
    "temporal_trend_fact": _build_temporal_trend_fact,
    "account_owner_factless": _build_account_owner_factless,
}


def build_staging_for_dataset(
    spark: SparkSession,
    dataset_name: str,
    input_base_path: str,
    output_base_path: str,
    batch_logical_date: str,
    gold_cache: dict[str, DataFrame] | None = None,
    options: dict | None = None,
) -> tuple[DataFrame, dict[str, DataFrame]]:
    """
    Build and write one dataset's staging output.

    This is an Airflow-friendly task boundary: a DAG can run this for a single
    dataset, then call publish_dataset once dependencies are satisfied.
    """
    if dataset_name not in DATASETS:
        raise ValueError(f"Unknown Gold dataset: {dataset_name}")

    options = options or {}
    gold_cache = gold_cache if gold_cache is not None else {}
    contract = DATASETS[dataset_name]
    _enforce_dashboard_sla(dataset_name, options)

    sources = _read_sources(spark, contract, input_base_path, options)
    dependencies = _read_dependencies(spark, contract, output_base_path, gold_cache)

    builder = BUILDERS[dataset_name]
    staging_df = builder(
        spark,
        sources,
        dependencies,
        batch_logical_date,
        options,
    )
    run_context = _build_gold_run_context(spark, batch_logical_date, options)
    options["_gold_run_context"] = run_context
    _, staging_df = audit_write_dataset(
        staging_df,
        output_base_path,
        contract,
        run_context,
    )
    return staging_df, sources


def build_staging_task(
    spark: SparkSession,
    dataset_name: str,
    input_base_path: str,
    output_base_path: str,
    batch_logical_date: str,
    options: dict | None = None,
) -> str:
    """
    Airflow-friendly staging task: build one dataset and return only its path.

    This avoids passing a Spark DataFrame between scheduler tasks. A later
    publish task can read the staging path and run Audit 2 independently.
    """
    build_staging_for_dataset(
        spark=spark,
        dataset_name=dataset_name,
        input_base_path=input_base_path,
        output_base_path=output_base_path,
        batch_logical_date=batch_logical_date,
        options=options,
    )
    return _staging_path(output_base_path, DATASETS[dataset_name])


def publish_dataset(
    dataset_name: str,
    staging_df: DataFrame,
    output_base_path: str,
    source_dfs: dict[str, DataFrame] | None = None,
    batch_logical_date: str | None = None,
    options: dict | None = None,
) -> tuple[str, list]:
    """
    Audit and publish one staged Gold dataset.

    This is the second Airflow-friendly task boundary. Full-load tables overwrite,
    incremental tables append/partition-overwrite, and CDC tables upsert according
    to their contract.
    """
    if dataset_name not in DATASETS:
        raise ValueError(f"Unknown Gold dataset: {dataset_name}")

    options = options or {}
    contract = DATASETS[dataset_name]
    run_context = options.get("_gold_run_context")
    if run_context is None:
        run_context = _build_gold_run_context(
            staging_df.sparkSession,
            batch_logical_date or options.get("batch_logical_date"),
            options,
        )
    audit_context = _build_audit_log_context(
        dataset_name=dataset_name,
        contract=contract,
        output_base_path=output_base_path,
        batch_logical_date=batch_logical_date,
        options=options,
    )

    return audit_publish_dataset(
        staging_df=staging_df,
        output_base_path=output_base_path,
        contract=contract,
        run_context=run_context,
        source_dfs=source_dfs,
        audit_log_context=audit_context,
        audit_log_path=quality_audit_log_path(output_base_path),
    )


def _build_audit_log_context(
    dataset_name: str,
    contract: GoldDatasetContract,
    output_base_path: str,
    batch_logical_date: str | None,
    options: dict,
) -> AuditLogContext:
    return AuditLogContext(
        pipeline_name="silver_to_gold",
        job_name="silver_to_gold",
        layer="gold",
        dataset_name=dataset_name,
        table_name=dataset_name,
        quality_stage="AUDIT_2",
        batch_logical_date=batch_logical_date,
        window_start=options.get("window_start"),
        window_end=options.get("window_end"),
        load_strategy=str(contract.load_strategy),
        staging_path=_staging_path(output_base_path, contract),
        publish_path=_gold_path(output_base_path, contract),
    )


def _build_gold_run_context(
    spark: SparkSession,
    batch_logical_date: str | None,
    options: dict,
) -> GoldRunContext:
    """
    Business rule: every Gold staging/publish attempt must be traceable to one
    logical pipeline run and, for dashboard datasets, one bounded refresh window.
    """
    if not batch_logical_date:
        raise ValueError("Gold run context requires batch_logical_date")

    return GoldRunContext.from_spark(
        spark=spark,
        batch_logical_date=batch_logical_date,
        window_start=options.get("window_start"),
        window_end=options.get("window_end"),
    )


def publish_dataset_from_staging(
    spark: SparkSession,
    dataset_name: str,
    input_base_path: str,
    output_base_path: str,
    batch_logical_date: str | None = None,
    options: dict | None = None,
) -> tuple[str, list]:
    """
    Airflow-friendly publish task: read staging from storage, audit, publish.

    The source read is repeated intentionally so Audit 2 can reconcile staging
    against the same bounded source window in an independent Spark job.
    """
    if dataset_name not in DATASETS:
        raise ValueError(f"Unknown Gold dataset: {dataset_name}")

    options = options or {}
    contract = DATASETS[dataset_name]
    _enforce_dashboard_sla(dataset_name, options)

    staging_df = read_staging_dataset(spark, output_base_path, contract)
    source_dfs = _read_sources(spark, contract, input_base_path, options)

    return publish_dataset(
        dataset_name=dataset_name,
        staging_df=staging_df,
        output_base_path=output_base_path,
        source_dfs=source_dfs,
        batch_logical_date=batch_logical_date,
        options=options,
    )


def _run_dataset(
    spark: SparkSession,
    dataset_name: str,
    config: GoldDatasetContract,
    input_base_path: str,
    output_base_path: str,
    batch_logical_date: str,
    gold_cache: dict[str, DataFrame] | None = None,
    options: dict | None = None,
) -> str:
    """
    Run one dataset end-to-end for local/EMR use.

    Airflow can call build_staging_for_dataset and publish_dataset separately;
    this wrapper is intentionally thin for current non-Airflow execution.
    """
    staging_df, sources = build_staging_for_dataset(
        spark=spark,
        dataset_name=dataset_name,
        input_base_path=input_base_path,
        output_base_path=output_base_path,
        batch_logical_date=batch_logical_date,
        gold_cache=gold_cache,
        options=options,
    )
    output_path, audit_results = publish_dataset(
        dataset_name=dataset_name,
        staging_df=staging_df,
        output_base_path=output_base_path,
        source_dfs=sources,
        batch_logical_date=batch_logical_date,
        options=options,
    )

    failed_warnings = [
        result.check_name
        for result in audit_results
        if result.status == "FAILED" and result.severity.upper() == "WARNING"
    ]
    if failed_warnings:
        logger.warning(
            "Published %s with warning audit failures: %s",
            dataset_name,
            ", ".join(failed_warnings),
        )

    if gold_cache is not None:
        gold_cache[dataset_name] = staging_df

    logger.info("Processed %s -> %s", dataset_name, output_path)
    return output_path


def process_silver_to_gold(
    spark: SparkSession,
    input_base_path: str,
    output_base_path: str,
    batch_logical_date: str,
    date_start: str | None = None,
    date_end: str | None = None,
    currencies: list[str] | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> dict[str, str]:
    """
    Local/EMR runner. Airflow should normally call _run_dataset per task.

    Full-load dimensions run without a 5-minute window. Dashboard-facing
    datasets require window_start/window_end so refresh work stays bounded.
    """
    options = {
        "date_start": date_start,
        "date_end": date_end,
        "currencies": currencies,
        "window_start": window_start,
        "window_end": window_end,
    }
    gold_cache: dict[str, DataFrame] = {}
    output_paths: dict[str, str] = {}

    for dataset_name in PIPELINE_ORDER:
        if dataset_name in DASHBOARD_DATASETS and not (window_start and window_end):
            logger.info(
                "Skipping %s because no 5-minute dashboard window was provided.",
                dataset_name,
            )
            continue

        output_paths[dataset_name] = _run_dataset(
            spark=spark,
            dataset_name=dataset_name,
            config=DATASETS[dataset_name],
            input_base_path=input_base_path,
            output_base_path=output_base_path,
            batch_logical_date=batch_logical_date,
            gold_cache=gold_cache,
            options=options,
        )

    return output_paths
