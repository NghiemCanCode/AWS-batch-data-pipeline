from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)


QualityAuditLogSchema = StructType(
    [
        StructField("audit_event_id", StringType(), False),
        StructField("audit_run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("job_name", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("dataset_name", StringType(), False),
        StructField("table_name", StringType(), True),
        StructField("quality_pattern", StringType(), False),
        StructField("quality_stage", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_group", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("message", StringType(), True),
        StructField("expected", StringType(), True),
        StructField("actual", StringType(), True),
        StructField("batch_logical_date", StringType(), True),
        StructField("window_start", StringType(), True),
        StructField("window_end", StringType(), True),
        StructField("load_strategy", StringType(), True),
        StructField("staging_path", StringType(), True),
        StructField("publish_path", StringType(), True),
        StructField("processing_id", StringType(), True),
        StructField("created_at", TimestampNTZType(), False),
        StructField("dt", StringType(), False),
    ]
)
