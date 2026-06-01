from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from .common import read_table_or_path
from .contracts import GoldDatasetContract, GoldLoadStrategy


logger = logging.getLogger(__name__)


def write_staging_dataset(
    df: DataFrame,
    output_base_path: str,
    contract: GoldDatasetContract,
    mode: str = "overwrite",
) -> str:
    path = _resolved_path(output_base_path, contract.staging_path)
    _write_parquet(df, path, mode=mode, partition_columns=contract.partition_columns)
    logger.info("[AWAP WRITE] %s staging -> %s", contract.name, path)
    return path


def publish_staging_dataset(
    staging_df: DataFrame,
    output_base_path: str,
    contract: GoldDatasetContract,
) -> str:
    path = _resolved_path(output_base_path, contract.publish_path)

    if contract.load_strategy == GoldLoadStrategy.FULL:
        _write_parquet(
            staging_df,
            path,
            mode="overwrite",
            partition_columns=contract.partition_columns,
        )
    elif contract.load_strategy == GoldLoadStrategy.INCREMENTAL_APPEND:
        _write_parquet(
            staging_df,
            path,
            mode="append",
            partition_columns=contract.partition_columns,
        )
    elif contract.load_strategy == GoldLoadStrategy.PARTITION_OVERWRITE:
        _publish_partition_overwrite(staging_df, path, contract)
    elif contract.load_strategy == GoldLoadStrategy.CDC_UPSERT:
        _publish_cdc_upsert(staging_df, path, contract)
    else:
        raise ValueError(f"Unsupported load strategy: {contract.load_strategy}")

    logger.info("[AWAP PUBLISH] %s -> %s", contract.name, path)
    return path


def read_published_or_empty(
    staging_df: DataFrame,
    target_path: str,
    contract: GoldDatasetContract,
) -> DataFrame:
    try:
        return read_table_or_path(staging_df.sparkSession, target_path)
    except Exception:
        return staging_df.sparkSession.createDataFrame([], schema=contract.schema)


def _publish_partition_overwrite(
    staging_df: DataFrame,
    target_path: str,
    contract: GoldDatasetContract,
) -> None:
    if not contract.partition_columns:
        raise ValueError(
            f"{contract.name} uses partition_overwrite but has no partition columns"
        )

    spark = staging_df.sparkSession
    previous_mode = spark.conf.get(
        "spark.sql.sources.partitionOverwriteMode",
        "static",
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        _write_parquet(
            staging_df,
            target_path,
            mode="overwrite",
            partition_columns=contract.partition_columns,
        )
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", previous_mode)


def _publish_cdc_upsert(
    staging_df: DataFrame,
    target_path: str,
    contract: GoldDatasetContract,
) -> None:
    if not contract.cdc_key:
        raise ValueError(f"{contract.name} uses cdc_upsert but has no cdc_key")

    existing_df = read_published_or_empty(staging_df, target_path, contract)
    affected_keys = staging_df.select(*contract.cdc_key).distinct()

    existing_untouched = existing_df.join(
        affected_keys,
        on=list(contract.cdc_key),
        how="left_anti",
    )
    merged_df = existing_untouched.unionByName(staging_df, allowMissingColumns=True)

    _write_parquet(
        merged_df,
        target_path,
        mode="overwrite",
        partition_columns=contract.partition_columns,
    )


def _write_parquet(
    df: DataFrame,
    path: str,
    mode: str,
    partition_columns: tuple[str, ...] = (),
) -> None:
    writer = df.write.mode(mode).format("parquet")
    available_partitions = [
        column for column in partition_columns if column in df.columns
    ]
    if available_partitions:
        writer = writer.partitionBy(*available_partitions)

    writer.save(path)


def _resolved_path(output_base_path: str, relative_path: str) -> str:
    return f"{output_base_path.rstrip('/')}/{relative_path.lstrip('/')}"
