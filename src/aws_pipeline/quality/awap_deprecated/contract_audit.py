from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ...utils.audit_helpers import AuditResult, audit_fail, audit_pass
from .common import (
    audit_schema_contract,
    dashboard_refresh_sla_minutes,
    has_critical_failures,
)
from .contracts import GoldDatasetContract


def audit_staging_contract(
    staging_df: DataFrame,
    contract: GoldDatasetContract,
    source_dfs: dict[str, DataFrame] | None = None,
) -> list[AuditResult]:
    results: list[AuditResult] = []

    schema_result = audit_schema_contract(staging_df, contract.schema)
    results.append(schema_result)
    if schema_result.status == "FAILED":
        return results

    metrics = _staging_metrics(staging_df, contract)
    if not _append_not_empty(results, metrics["row_count"], contract.name):
        return results

    _append_required_not_null_checks(results, metrics, contract)
    _append_unique_key_check(results, staging_df, contract)
    _append_freshness_check(results, metrics, contract)

    if source_dfs and contract.row_count_source:
        _append_row_count_reconciliation(
            results,
            staging_count=metrics["row_count"],
            source_df=source_dfs[contract.row_count_source],
            contract=contract,
        )

    if source_dfs:
        _append_sum_reconciliations(results, metrics, source_dfs, contract)

    _append_publish_gate(results)
    return results


def _staging_metrics(staging_df: DataFrame, contract: GoldDatasetContract) -> dict:
    aggregations = [
        F.count("*").alias("row_count"),
    ]

    if contract.freshness_column in staging_df.columns:
        aggregations.append(
            F.max(contract.freshness_column).alias("max_freshness_value")
        )

    for rule in contract.reconciliation_rules:
        if rule.target_column and rule.target_column in staging_df.columns:
            aggregations.append(
                F.sum(F.col(rule.target_column)).alias(f"{rule.name}_target_sum")
            )

    for column in contract.required_columns:
        aggregations.append(
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(
                f"{column}_nulls"
            )
        )

    return staging_df.agg(*aggregations).first().asDict()


def _append_not_empty(
    results: list[AuditResult],
    row_count: int,
    dataset_name: str,
) -> bool:
    if row_count == 0:
        results.append(
            audit_fail(
                name="staging_not_empty",
                group="readiness",
                message=f"{dataset_name} staging is empty",
                expected="at least 1 row",
                actual="0 rows",
            )
        )
        return False

    results.append(audit_pass("staging_not_empty", "readiness"))
    return True


def _append_required_not_null_checks(
    results: list[AuditResult],
    metrics: dict,
    contract: GoldDatasetContract,
) -> None:
    for column in contract.required_columns:
        null_count = metrics[f"{column}_nulls"]
        if null_count > 0:
            results.append(
                audit_fail(
                    name=f"{column}_not_null",
                    group="nulls",
                    message=f"{column} contains null values in staging",
                    expected="0 null rows",
                    actual=str(null_count),
                )
            )
        else:
            results.append(audit_pass(f"{column}_not_null", "nulls"))


def _append_unique_key_check(
    results: list[AuditResult],
    staging_df: DataFrame,
    contract: GoldDatasetContract,
) -> None:
    if not contract.unique_key:
        return

    duplicate_count = (
        staging_df.groupBy(*contract.unique_key)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_count > 0:
        results.append(
            audit_fail(
                name="unique_key",
                group="uniqueness",
                message=f"Duplicate unique key found for {contract.name}",
                expected="0 duplicate keys",
                actual=">= 1",
            )
        )
    else:
        results.append(audit_pass("unique_key", "uniqueness"))


def _append_freshness_check(
    results: list[AuditResult],
    metrics: dict,
    contract: GoldDatasetContract,
) -> None:
    max_freshness_value = metrics.get("max_freshness_value")
    if max_freshness_value is None:
        results.append(
            audit_fail(
                name="staging_freshness",
                group="freshness",
                message=f"max({contract.freshness_column}) is null or unavailable",
            )
        )
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lag_minutes = (now - max_freshness_value).total_seconds() / 60
    if lag_minutes > contract.max_window_minutes:
        results.append(
            audit_fail(
                name="staging_freshness",
                group="freshness",
                message="Gold staging is too stale for dashboard refresh",
                expected=f"<= {dashboard_refresh_sla_minutes()} minutes",
                actual=f"{lag_minutes:.2f} minutes",
            )
        )
    else:
        results.append(audit_pass("staging_freshness", "freshness"))


def _append_row_count_reconciliation(
    results: list[AuditResult],
    staging_count: int,
    source_df: DataFrame,
    contract: GoldDatasetContract,
) -> None:
    source_count = source_df.count()
    lost_rows = source_count - staging_count
    loss_pct = lost_rows / source_count * 100 if source_count else 0

    if source_count == 0:
        results.append(
            audit_fail(
                name="row_count_reconciliation",
                group="reconciliation",
                message="Source is empty during staging reconciliation",
                expected="source rows > 0",
                actual="0 rows",
            )
        )
    elif lost_rows > 0 and loss_pct > contract.max_join_loss_pct:
        results.append(
            audit_fail(
                name="row_count_reconciliation",
                group="reconciliation",
                message="Staging row count is lower than source count",
                expected=f"loss <= {contract.max_join_loss_pct:.4f}%",
                actual=(
                    f"source={source_count}, staging={staging_count}, "
                    f"loss={loss_pct:.4f}%"
                ),
            )
        )
    elif lost_rows < 0:
        results.append(
            audit_fail(
                name="row_count_reconciliation",
                group="reconciliation",
                message="Staging has more rows than source",
                expected=f"staging <= source ({source_count})",
                actual=str(staging_count),
            )
        )
    else:
        results.append(audit_pass("row_count_reconciliation", "reconciliation"))


def _append_sum_reconciliations(
    results: list[AuditResult],
    metrics: dict,
    source_dfs: dict[str, DataFrame],
    contract: GoldDatasetContract,
) -> None:
    for rule in contract.reconciliation_rules:
        if not rule.source_column or not rule.target_column:
            continue

        source_sum = source_dfs[rule.source_name].agg(
            F.sum(F.col(rule.source_column).cast("decimal(18,2)")).alias("source_sum")
        ).first()["source_sum"]
        target_sum = metrics.get(f"{rule.name}_target_sum")

        if source_sum is None or target_sum is None:
            results.append(
                audit_fail(
                    name=rule.name,
                    group="reconciliation",
                    message="Cannot compare sums because one side is null",
                    expected="non-null sums",
                    actual=f"source={source_sum}, target={target_sum}",
                )
            )
            continue

        diff = abs(float(source_sum) - float(target_sum))
        tolerance = max(
            rule.tolerance_absolute,
            abs(float(source_sum)) * rule.tolerance_pct / 100,
        )
        if diff > tolerance:
            results.append(
                audit_fail(
                    name=rule.name,
                    group="reconciliation",
                    message="Target sum does not match source sum",
                    expected=f"diff <= {tolerance:.4f}",
                    actual=f"diff={diff:.4f}",
                )
            )
        else:
            results.append(audit_pass(rule.name, "reconciliation"))


def _append_publish_gate(results: list[AuditResult]) -> None:
    if has_critical_failures(results):
        results.append(
            audit_fail(
                name="publish_gate",
                group="publish",
                message="Critical Audit 2 failures block publish",
                expected="no critical failures",
                actual="critical failures found",
            )
        )
    else:
        results.append(audit_pass("publish_gate", "publish"))
