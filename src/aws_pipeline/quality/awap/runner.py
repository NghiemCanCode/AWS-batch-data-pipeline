from __future__ import annotations

from time import monotonic

from pyspark.sql import DataFrame

from ..audit_log import AuditLogContext, write_quality_audit_log
from ...utils.audit_helpers import AuditResult, audit_fail
from .common import has_critical_failures
from .contract_audit import audit_staging_contract
from .contracts import GoldDatasetContract
from .publisher import publish_staging_dataset, write_staging_dataset


def audit_write_dataset(
    staging_df: DataFrame,
    output_base_path: str,
    contract: GoldDatasetContract,
) -> str:
    """
    Generic Write step for any dataset after its input audit/transform.
    """
    return write_staging_dataset(
        staging_df,
        output_base_path=output_base_path,
        contract=contract,
        mode="overwrite",
    )


def audit_publish_dataset(
    staging_df: DataFrame,
    output_base_path: str,
    contract: GoldDatasetContract,
    source_dfs: dict[str, DataFrame] | None = None,
    audit_log_context: AuditLogContext | None = None,
    audit_log_path: str | None = None,
) -> tuple[str, list[AuditResult]]:
    """
    Generic Audit 2 + Publish step for full, incremental, and CDC datasets.
    """
    started_at = monotonic()
    audit_results = audit_staging_contract(
        staging_df=staging_df,
        contract=contract,
        source_dfs=source_dfs,
    )

    if has_critical_failures(audit_results):
        _write_audit_log_if_configured(
            staging_df,
            audit_results,
            audit_log_context,
            audit_log_path,
        )
        failed_checks = [
            result.check_name
            for result in audit_results
            if result.status == "FAILED" and result.severity.upper() == "CRITICAL"
        ]
        raise ValueError(
            f"{contract.name} Audit 2 failed; publish skipped: "
            + ", ".join(failed_checks)
        )

    output_path = publish_staging_dataset(
        staging_df,
        output_base_path=output_base_path,
        contract=contract,
    )

    elapsed_seconds = monotonic() - started_at
    if elapsed_seconds > contract.publish_budget_seconds:
        audit_results.append(
            audit_fail(
                name="publish_latency_budget",
                group="sla",
                message="Audit 2 + publish exceeded the dashboard latency budget",
                expected=f"<= {contract.publish_budget_seconds} seconds",
                actual=f"{elapsed_seconds:.2f} seconds",
                severity="WARNING",
            )
        )

    _write_audit_log_if_configured(
        staging_df,
        audit_results,
        audit_log_context,
        audit_log_path,
    )
    return output_path, audit_results


def _write_audit_log_if_configured(
    staging_df: DataFrame,
    audit_results: list[AuditResult],
    audit_log_context: AuditLogContext | None,
    audit_log_path: str | None,
) -> None:
    if audit_log_context is None or audit_log_path is None:
        return

    write_quality_audit_log(
        spark=staging_df.sparkSession,
        audit_results=audit_results,
        context=audit_log_context,
        output_path=audit_log_path,
    )
