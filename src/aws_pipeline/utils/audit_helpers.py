from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def schema_enforcing(df: DataFrame, schema: StructType) -> DataFrame:
    select_cols = []

    for field in schema.fields:
        if field.name not in df.columns:
            logger.warning(
                f"[SCHEMA] Column '{field.name}' missing → inserting NULL as {field.dataType}."
            )
            select_cols.append(F.lit(None).cast(field.dataType).alias(field.name))

        elif df.schema[field.name].dataType == field.dataType:
            select_cols.append(F.col(field.name))

        else:
            logger.warning(
                f"[SCHEMA] Column '{field.name}': "
                f"expected {field.dataType}, got {df.schema[field.name].dataType} "
                f"→ applying try_cast."
            )
            select_cols.append(
                F.expr(f"try_cast(`{field.name}` as {field.dataType.simpleString()})").alias(field.name)
            )

    return df.select(select_cols)


def add_audit_columns(df: DataFrame, batch_logical_date: str) -> DataFrame:
    app_id = df.sparkSession.sparkContext.applicationId
    now = F.current_timestamp()
    return (
        df.withColumn("_created_at", now)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_processing_id", F.lit(app_id))
        .withColumn("_updated_at", now)
        .withColumn("_batch_logical_date", F.lit(batch_logical_date))
        .withColumn("_is_deleted", F.lit(False))
    )
