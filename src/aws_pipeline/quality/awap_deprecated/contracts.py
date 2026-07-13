from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
import os
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from ..config import get_sla_config


class GoldLoadStrategy(StrEnum):
    FULL = "full"
    INCREMENTAL_APPEND = "incremental_append"
    PARTITION_OVERWRITE = "partition_overwrite"
    CDC_UPSERT = "cdc_upsert"


class RefreshCadence(StrEnum):
    ONE_TIME = "one_time"
    FULL_REFRESH = "full_refresh"
    CDC = "cdc"
    FIVE_MINUTES = "five_minutes"
    PERIODIC = "periodic"


@dataclass(frozen=True)
class GoldRunContext:
    batch_logical_date: str
    pipeline_run_id: str
    audit_run_id: str
    lineage_run_id: str | None = None
    window_start: str | None = None
    window_end: str | None = None

    @classmethod
    def from_spark(
        cls,
        spark: SparkSession,
        batch_logical_date: str,
        window_start: str | None = None,
        window_end: str | None = None,
    ) -> "GoldRunContext":
        spark_app_id = spark.sparkContext.applicationId or "local-spark-application"
        pipeline_run_id = os.environ.get("PIPELINE_RUN_ID") or spark_app_id
        audit_run_id = os.environ.get("AUDIT_RUN_ID") or str(uuid4())
        lineage_run_id = os.environ.get("LINEAGE_RUN_ID") or pipeline_run_id
        return cls(
            batch_logical_date=batch_logical_date,
            pipeline_run_id=pipeline_run_id,
            audit_run_id=audit_run_id,
            lineage_run_id=lineage_run_id,
            window_start=window_start,
            window_end=window_end,
        )


@dataclass(frozen=True)
class ReconciliationRule:
    name: str
    source_name: str
    source_column: str | None = None
    target_column: str | None = None
    tolerance_pct: float = 0.001
    tolerance_absolute: float = 0.01


@dataclass(frozen=True)
class GoldDatasetContract:
    name: str
    schema: StructType
    staging_path: str
    publish_path: str
    load_strategy: GoldLoadStrategy
    refresh_cadence: RefreshCadence = RefreshCadence.PERIODIC
    description: str | None = None
    source_names: tuple[str, ...] = ()
    dependency_names: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    unique_key: tuple[str, ...] = ()
    partition_columns: tuple[str, ...] = ()
    cdc_key: tuple[str, ...] = ()
    scd_natural_key: tuple[str, ...] = ()
    scd_tracked_columns: tuple[str, ...] = ()
    freshness_column: str = "_updated_at"
    max_window_minutes: int = field(
        default_factory=lambda: get_sla_config().dashboard_refresh_minutes
    )
    publish_budget_seconds: int = field(
        default_factory=lambda: get_sla_config().publish_budget_seconds
    )
    max_join_loss_pct: float = 0.1
    row_count_source: str | None = None
    reconciliation_rules: tuple[ReconciliationRule, ...] = ()
    transform_func: Callable[..., DataFrame] | None = None
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def is_full_load(self) -> bool:
        return self.load_strategy == GoldLoadStrategy.FULL

    @property
    def is_incremental(self) -> bool:
        return self.load_strategy in {
            GoldLoadStrategy.INCREMENTAL_APPEND,
            GoldLoadStrategy.PARTITION_OVERWRITE,
            GoldLoadStrategy.CDC_UPSERT,
        }

    @property
    def requires_dashboard_window(self) -> bool:
        return self.refresh_cadence == RefreshCadence.FIVE_MINUTES
