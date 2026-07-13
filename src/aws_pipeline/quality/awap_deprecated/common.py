from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import get_sla_config
from ...utils.audit_helpers import AuditResult, audit_fail, audit_pass


DASHBOARD_REFRESH_SLA_MINUTES = get_sla_config().dashboard_refresh_minutes


def dashboard_refresh_sla_minutes() -> int:
    return get_sla_config().dashboard_refresh_minutes


def schema_type_map(df: DataFrame) -> dict[str, Any]:
    return {field.name: field.dataType for field in df.schema.fields}


def expected_schema_type_map(schema) -> dict[str, Any]:
    return {field.name: field.dataType for field in schema.fields}


def has_critical_failures(results: list[AuditResult]) -> bool:
    return any(
        result.status == "FAILED" and result.severity.upper() == "CRITICAL"
        for result in results
    )


def timestamp_lit(value: str):
    return F.lit(value).cast("timestamp_ntz")


def read_table_or_path(spark: SparkSession, table_or_path: str) -> DataFrame:
    try:
        return spark.table(table_or_path)
    except Exception:
        return spark.read.parquet(table_or_path)


def validate_window_sla(window_start: str, window_end: str) -> tuple[bool, str]:
    start = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
    end = datetime.fromisoformat(window_end.replace("Z", "+00:00"))
    duration_minutes = (end - start).total_seconds() / 60

    if duration_minutes <= 0:
        return False, f"{duration_minutes:.2f} minutes"

    if duration_minutes > dashboard_refresh_sla_minutes():
        return False, f"{duration_minutes:.2f} minutes"

    return True, f"{duration_minutes:.2f} minutes"


def audit_schema_contract(
    df: DataFrame,
    expected_schema,
    check_name: str = "schema_contract",
) -> AuditResult:
    actual_schema = schema_type_map(df)
    expected_schema_types = expected_schema_type_map(expected_schema)
    schema_errors = []

    for column, expected_type in expected_schema_types.items():
        if column not in actual_schema:
            schema_errors.append(f"Missing column: {column}")
        elif actual_schema[column] != expected_type:
            schema_errors.append(
                f"Invalid type for {column}: "
                f"expected={expected_type}, actual={actual_schema[column]}"
            )

    if schema_errors:
        return audit_fail(
            name=check_name,
            group="schema",
            message="; ".join(schema_errors),
        )

    return audit_pass(check_name, "schema")


def audit_window_sla() -> AuditResult:
    return audit_pass("dashboard_refresh_window", "sla")


def audit_window_sla_failure(window_duration: str) -> AuditResult:
    return audit_fail(
        name="dashboard_refresh_window",
        group="sla",
        message="Window duration does not satisfy dashboard refresh SLA",
        expected=f"0 < window <= {dashboard_refresh_sla_minutes()} minutes",
        actual=window_duration,
    )
