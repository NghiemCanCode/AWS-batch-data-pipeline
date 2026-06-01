from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pyspark.sql import DataFrame
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
