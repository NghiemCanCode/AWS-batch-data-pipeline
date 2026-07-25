from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import SparkSession

from ..schemas.platform_schema import QualityAuditLogSchema
from ..utils.audit_helpers import AuditResult


QUALITY_AUDIT_LOG_RELATIVE_PATH = "platform/audit/quality_audit_log"


@dataclass(frozen=True)
class AuditLogContext:
    pipeline_name: str
    dataset_name: str
    quality_stage: str
    audit_run_id: str = field(default_factory=lambda: str(uuid4()))
    job_name: str | None = None
    layer: str | None = None
    table_name: str | None = None
    quality_pattern: str = "AWAP"
    batch_logical_date: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    load_strategy: str | None = None
    staging_path: str | None = None
    publish_path: str | None = None


def quality_audit_log_path(base_path: str) -> str:
    return f"{base_path.rstrip('/')}/{QUALITY_AUDIT_LOG_RELATIVE_PATH}"


def write_quality_audit_log(
    spark: SparkSession,
    audit_results: list[AuditResult],
    context: AuditLogContext,
    output_path: str,
) -> None:
    if not audit_results:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    dt = now.date().isoformat()
    processing_id = spark.sparkContext.applicationId

    rows = [
        (
            str(uuid4()),
            context.audit_run_id,
            context.pipeline_name,
            context.job_name,
            context.layer,
            context.dataset_name,
            context.table_name,
            context.quality_pattern,
            context.quality_stage,
            result.check_name,
            result.check_group,
            result.severity.upper(),
            result.status.upper(),
            result.message,
            result.expected,
            result.actual,
            context.batch_logical_date,
            context.window_start,
            context.window_end,
            context.load_strategy,
            context.staging_path,
            context.publish_path,
            processing_id,
            now,
            dt,
        )
        for result in audit_results
    ]

    audit_df = spark.createDataFrame(rows, schema=QualityAuditLogSchema)
    (
        audit_df.write.mode("append")
        .format("parquet")
        .partitionBy("dt", "pipeline_name", "dataset_name")
        .save(output_path)
    )
