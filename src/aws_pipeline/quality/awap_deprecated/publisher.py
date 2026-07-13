from __future__ import annotations

import logging
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .contracts import GoldDatasetContract, GoldLoadStrategy, GoldRunContext


logger = logging.getLogger(__name__)

ICEBERG_CATALOG = "glue_catalog"
GOLD_DATABASE = "gold"
GOLD_STAGING_DATABASE = "gold_staging"
GOLD_ICEBERG_PREFIX = "gold/iceberg"


def write_staging_dataset(
    df: DataFrame,
    output_base_path: str,
    contract: GoldDatasetContract,
    run_context: GoldRunContext,
    mode: str = "overwrite",
) -> tuple[str, DataFrame]:
    table_name = staging_table_identifier(contract)
    staging_df = add_gold_audit_columns(df, contract, run_context)
    _ensure_iceberg_catalog(staging_df, output_base_path)
    _ensure_table_exists(
        staging_df,
        table_name,
        contract.schema,
        partition_columns=contract.partition_columns,
    )

    if mode != "overwrite":
        raise ValueError(f"Unsupported Gold staging write mode: {mode}")

    staging_df.writeTo(table_name).overwrite(F.lit(True))
    logger.info("[AWAP WRITE] %s staging -> %s", contract.name, table_name)
    return table_name, staging_df


def publish_staging_dataset(
    staging_df: DataFrame,
    output_base_path: str,
    contract: GoldDatasetContract,
    run_context: GoldRunContext,
) -> str:
    table_name = published_table_identifier(contract)
    _ensure_iceberg_catalog(staging_df, output_base_path)
    _ensure_table_exists(
        staging_df,
        table_name,
        contract.schema,
        partition_columns=contract.partition_columns,
    )

    if contract.load_strategy == GoldLoadStrategy.FULL:
        staging_df.writeTo(table_name).overwrite(F.lit(True))
    elif contract.load_strategy == GoldLoadStrategy.INCREMENTAL_APPEND:
        staging_df.writeTo(table_name).append()
    elif contract.load_strategy == GoldLoadStrategy.PARTITION_OVERWRITE:
        _publish_partition_overwrite(staging_df, table_name, contract)
    elif contract.load_strategy == GoldLoadStrategy.CDC_UPSERT:
        if contract.scd_natural_key and contract.scd_tracked_columns:
            _publish_scd2_upsert(staging_df, table_name, contract)
        else:
            _publish_cdc_upsert(staging_df, table_name, contract)
    else:
        raise ValueError(f"Unsupported load strategy: {contract.load_strategy}")

    logger.info(
        "[AWAP PUBLISH] %s -> %s run_id=%s audit_run_id=%s",
        contract.name,
        table_name,
        run_context.pipeline_run_id,
        run_context.audit_run_id,
    )
    return table_name


def add_gold_audit_columns(
    df: DataFrame,
    contract: GoldDatasetContract,
    run_context: GoldRunContext,
) -> DataFrame:
    """
    Business rule: Gold audit metadata is run-centric because Gold rows are
    derived from joins, aggregates, and CDC merges, not from a single physical
    source file. Source-file lineage belongs in Bronze/Silver ingestion logs.
    """
    now = F.current_timestamp().cast("timestamp_ntz")
    with_audit = (
        df.withColumn("_created_at", now)
        .withColumn("_updated_at", now)
        .withColumn(
            "_batch_logical_date",
            F.lit(run_context.batch_logical_date).cast("timestamp_ntz"),
        )
        .withColumn(
            "_processing_id",
            F.lit(_spark_application_id(df)).cast("string"),
        )
        .withColumn(
            "_pipeline_run_id",
            F.lit(run_context.pipeline_run_id).cast("string"),
        )
        .withColumn(
            "_audit_run_id",
            F.lit(run_context.audit_run_id).cast("string"),
        )
        .withColumn(
            "_lineage_run_id",
            F.lit(run_context.lineage_run_id).cast("string"),
        )
        .withColumn("_is_deleted", F.lit(False))
    )
    return with_audit.select(*contract.schema.fieldNames())


def read_staging_dataset(
    spark,
    output_base_path: str,
    contract: GoldDatasetContract,
) -> DataFrame:
    configure_gold_iceberg_catalog(spark, output_base_path)
    return spark.table(staging_table_identifier(contract))


def configure_gold_iceberg_catalog(spark, output_base_path: str) -> None:
    _ensure_iceberg_catalog_for_spark(spark, output_base_path)


def staging_table_identifier(contract: GoldDatasetContract) -> str:
    return _table_identifier(GOLD_STAGING_DATABASE, contract.name)


def published_table_identifier(contract: GoldDatasetContract) -> str:
    return _table_identifier(GOLD_DATABASE, contract.name)


def iceberg_warehouse_uri(output_base_path: str) -> str:
    return f"{output_base_path.rstrip('/')}/{GOLD_ICEBERG_PREFIX}/"


def _publish_partition_overwrite(
    staging_df: DataFrame,
    table_name: str,
    contract: GoldDatasetContract,
) -> None:
    """
    Business rule: dashboard and periodic aggregate facts are deterministic for
    their partition grain, so publish replaces only partitions touched by the
    bounded staging data instead of rewriting all Gold history.
    """
    if not contract.partition_columns:
        raise ValueError(
            f"{contract.name} uses partition_overwrite but has no partition columns"
        )

    staging_df.writeTo(table_name).overwritePartitions()


def _publish_cdc_upsert(
    staging_df: DataFrame,
    table_name: str,
    contract: GoldDatasetContract,
) -> None:
    """
    Business rule: mutable non-SCD Gold tables use Type 1 CDC semantics. The
    latest source row replaces the published row for the CDC key while original
    row creation time remains stable for auditability.
    """
    if not contract.cdc_key:
        raise ValueError(f"{contract.name} uses cdc_upsert but has no cdc_key")

    view_name = _temp_view_name(contract.name, "cdc")
    _deduplicate_latest_by_keys(staging_df, contract.cdc_key).createOrReplaceTempView(
        view_name
    )
    spark = staging_df.sparkSession
    spark.sql(
        f"""
        MERGE INTO {_quote_table(table_name)} AS target
        USING {_quote_identifier(view_name)} AS source
        ON {_join_condition("target", "source", contract.cdc_key)}
        WHEN MATCHED THEN UPDATE SET {_type1_update_assignments(staging_df.columns)}
        WHEN NOT MATCHED THEN INSERT ({_column_list(staging_df.columns)})
        VALUES ({_source_value_list(staging_df.columns)})
        """
    )


def _publish_scd2_upsert(
    staging_df: DataFrame,
    table_name: str,
    contract: GoldDatasetContract,
) -> None:
    """
    Business rule: customer/account history uses SCD Type 2 so analytics can
    distinguish business-impacting profile/card changes over time. Unchanged
    retries are ignored, and a new version is inserted only when tracked fields
    change after the current version's effective_from_date.
    """
    _require_scd2_columns(staging_df, contract)

    spark = staging_df.sparkSession
    natural_keys = list(contract.scd_natural_key)
    tracked_columns = list(contract.scd_tracked_columns)
    staging_view = _temp_view_name(contract.name, "scd2_staging")
    insert_view = _temp_view_name(contract.name, "scd2_insert")

    scd2_staging_df = _deduplicate_latest_by_keys(
        staging_df,
        contract.scd_natural_key,
    )
    scd2_staging_df.createOrReplaceTempView(staging_view)
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW {_quote_identifier(insert_view)} AS
        SELECT {_scd2_insert_select_list(staging_df.columns)}
        FROM {_quote_identifier(staging_view)} AS source
        LEFT JOIN {_quote_table(table_name)} AS target
          ON {_join_condition("target", "source", natural_keys)}
         AND CAST(target.`is_current` AS BOOLEAN)
        WHERE target.`{natural_keys[0]}` IS NULL
           OR (
                ({_tracked_change_condition("target", "source", tracked_columns)})
            AND source.`effective_from_date` > target.`effective_from_date`
           )
        """
    )
    spark.sql(
        f"""
        MERGE INTO {_quote_table(table_name)} AS target
        USING {_quote_identifier(insert_view)} AS source
        ON {_join_condition("target", "source", natural_keys)}
       AND CAST(target.`is_current` AS BOOLEAN)
        WHEN MATCHED THEN UPDATE SET
          target.`effective_to_date` =
            source.`effective_from_date` - INTERVAL 1 MICROSECOND,
          target.`is_current` = false,
          target.`_updated_at` = CAST(current_timestamp() AS TIMESTAMP_NTZ)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {_quote_table(table_name)}
        ({_column_list(staging_df.columns)})
        SELECT {_column_list(staging_df.columns)}
        FROM {_quote_identifier(insert_view)}
        """
    )


def _require_scd2_columns(
    staging_df: DataFrame,
    contract: GoldDatasetContract,
) -> None:
    required_columns = (
        set(contract.scd_natural_key)
        | set(contract.scd_tracked_columns)
        | {"effective_from_date", "effective_to_date", "is_current", "version_number"}
    )
    missing_columns = sorted(required_columns - set(staging_df.columns))
    if missing_columns:
        raise ValueError(
            f"{contract.name} SCD2 publish missing column(s): "
            + ", ".join(missing_columns)
        )


def _deduplicate_latest_by_keys(
    df: DataFrame,
    keys: tuple[str, ...],
) -> DataFrame:
    order_columns = []
    for column in ("effective_from_date", "_updated_at", "_created_at"):
        if column in df.columns:
            order_columns.append(F.col(column).desc_nulls_last())

    if not order_columns:
        order_columns = [F.monotonically_increasing_id().desc()]

    window = Window.partitionBy(*keys).orderBy(*order_columns)
    return (
        df.withColumn("_awap_merge_rank", F.row_number().over(window))
        .where(F.col("_awap_merge_rank") == F.lit(1))
        .drop("_awap_merge_rank")
    )


def _ensure_iceberg_catalog(df: DataFrame, output_base_path: str) -> None:
    _ensure_iceberg_catalog_for_spark(df.sparkSession, output_base_path)


def _ensure_iceberg_catalog_for_spark(spark, output_base_path: str) -> None:
    spark.conf.set(
        f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
        iceberg_warehouse_uri(output_base_path),
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{_quote_table(ICEBERG_CATALOG + '.' + GOLD_DATABASE)}"
    )
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{_quote_table(ICEBERG_CATALOG + '.' + GOLD_STAGING_DATABASE)}"
    )


def _ensure_table_exists(
    df: DataFrame,
    table_name: str,
    schema: StructType,
    partition_columns: tuple[str, ...] = (),
) -> None:
    columns_sql = ",\n  ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        for field in schema.fields
    )
    partition_sql = ""
    available_partitions = [
        column for column in partition_columns if column in schema.fieldNames()
    ]
    if available_partitions:
        partition_sql = (
            "\nPARTITIONED BY ("
            + ", ".join(f"`{column}`" for column in available_partitions)
            + ")"
        )

    df.sparkSession.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_quote_table(table_name)} (
          {columns_sql}
        )
        USING iceberg{partition_sql}
        """
    )


def _type1_update_assignments(columns: list[str]) -> str:
    assignments = []
    for column in columns:
        if column == "_created_at":
            continue
        if column == "_updated_at":
            assignments.append(
                "target.`_updated_at` = CAST(current_timestamp() AS TIMESTAMP_NTZ)"
            )
            continue
        assignments.append(f"target.`{column}` = source.`{column}`")
    return ", ".join(assignments)


def _scd2_insert_select_list(columns: list[str]) -> str:
    expressions = []
    for column in columns:
        if column == "version_number":
            expressions.append(
                "CASE WHEN target.`version_number` IS NULL "
                "THEN CAST(source.`version_number` AS SMALLINT) "
                "ELSE CAST(target.`version_number` + 1 AS SMALLINT) "
                "END AS `version_number`"
            )
        elif column in {"_created_at", "_updated_at"}:
            expressions.append(
                f"CAST(current_timestamp() AS TIMESTAMP_NTZ) AS `{column}`"
            )
        else:
            expressions.append(f"source.`{column}` AS `{column}`")
    return ", ".join(expressions)


def _tracked_change_condition(
    left_alias: str,
    right_alias: str,
    columns: list[str],
) -> str:
    return " OR ".join(
        f"NOT ({left_alias}.`{column}` <=> {right_alias}.`{column}`)"
        for column in columns
    )


def _join_condition(
    left_alias: str,
    right_alias: str,
    columns: tuple[str, ...] | list[str],
) -> str:
    return " AND ".join(
        f"{left_alias}.`{column}` = {right_alias}.`{column}`"
        for column in columns
    )


def _column_list(columns: list[str]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def _source_value_list(columns: list[str]) -> str:
    return ", ".join(f"source.`{column}`" for column in columns)


def _table_identifier(database: str, table_name: str) -> str:
    return f"{ICEBERG_CATALOG}.{database}.{table_name}"


def _quote_table(table_name: str) -> str:
    return ".".join(_quote_identifier(part) for part in table_name.split("."))


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier}`"


def _temp_view_name(dataset_name: str, suffix: str) -> str:
    return f"tmp_{dataset_name}_{suffix}_{uuid4().hex}"


def _spark_application_id(df: DataFrame) -> str:
    return df.sparkSession.sparkContext.applicationId or "local-spark-application"
