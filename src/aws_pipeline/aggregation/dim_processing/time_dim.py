"""
This dimension is generated periodically at specific times and does not run concurrently with the main pipelines.
The TimeType() is not stable yet in this version of Spark, so we will use String type with Integer Smart Key instead.
"""

from pyspark.sql import SparkSession, DataFrame, functions as F

from ...utils.audit_helpers import schema_enforcing, add_audit_columns
from ...schemas.gold_schema import TimeDimensionSchema


def _calculate_time_key_col(column_name: str) -> F.col:
    return (
        F.floor(F.col(column_name) / F.lit(3600)) * F.lit(10000)
        + F.floor(((F.col(column_name) % F.lit(3600)) / F.lit(60))) * F.lit(100)
        + (F.col(column_name) % F.lit(60))
    )


def _calculate_time_24h_col(column_name: str) -> F.col:
    return F.concat(
        F.lpad(F.floor(F.col(column_name) / F.lit(10000)), 2, "0"),
        F.lit(":"),
        F.lpad(F.floor(((F.col(column_name) % F.lit(10000)) / F.lit(100))), 2, "0"),
        F.lit(":"),
        F.lpad((F.col(column_name) % F.lit(100)), 2, "0"),
    )


def _calculate_hour_24_col(column_name: str) -> F.col:
    return F.floor(F.col(column_name) / F.lit(10000)).cast("short")


def _calculate_hour_12_col(column_name: str) -> F.col:
    return F.when(
        F.floor(F.col(column_name) / F.lit(10000)) % F.lit(12) == F.lit(0),
        F.lit(12),
    ).otherwise(F.floor(F.col(column_name) / F.lit(10000)) % F.lit(12))


def _calculate_am_pm_col(column_name: str) -> F.col:
    return F.when(
        F.floor(F.col(column_name) / F.lit(10000)) < F.lit(12), "AM"
    ).otherwise("PM")


def _calculate_minute_col(column_name: str) -> F.col:
    return F.floor((F.col(column_name) % F.lit(10000)) / F.lit(100)).cast("short")


def _calculate_second_col(column_name: str) -> F.col:
    return (F.col(column_name) % F.lit(100)).cast("short")


def _calculate_time_bucket_col(column_name: str, bucket_size: int) -> F.col:
    hour = F.floor(F.col(column_name) / F.lit(10000))
    minute = F.floor(((F.col(column_name) % F.lit(10000)) / F.lit(100)))
    bucket_minute = F.floor(minute / F.lit(bucket_size)) * F.lit(bucket_size)

    return hour * F.lit(100) + bucket_minute


def _display_bucket_time_col(bucket_col_name: str) -> F.col:
    return F.concat(
        F.lpad(F.floor(F.col(bucket_col_name) / F.lit(100)), 2, "0"),
        F.lit(":"),
        F.lpad(F.floor(F.col(bucket_col_name) % F.lit(100)), 2, "0"),
    )


def _calculate_day_part_col(column_name: str) -> F.col:
    return (
        F.when(
            F.col(column_name).between(0, 59999),
            "Early Night",
        )
        .when(
            F.col(column_name).between(60000, 119999),
            "Morning",
        )
        .when(
            F.col(column_name).between(120000, 179999),
            "Afternoon",
        )
        .when(
            F.col(column_name).between(180000, 219999),
            "Evening",
        )
        .when(
            F.col(column_name).between(220000, 240000),
            "Night",
        )
    )


def generate_time_dim(spark: SparkSession, batch_logical_date) -> DataFrame:
    """
    Generate a time dimension table in 24 hours.

    Args:
        spark: The SparkSession to use for the operation.

    Returns:
        A DataFrame containing the time dimension table.
    """
    time_dim_df = spark.range(0, 86400, 1)
    time_dim_df = (
        time_dim_df.withColumn("time_key", _calculate_time_key_col("id"))
        .withColumn("time_24h", _calculate_time_24h_col("time_key"))
        .withColumn("hour_24", _calculate_hour_24_col("time_key"))
        .withColumn("hour_12", _calculate_hour_12_col("time_key"))
        .withColumn("am_pm", _calculate_am_pm_col("time_key"))
        .withColumn("minute", _calculate_minute_col("time_key"))
        .withColumn("second", _calculate_second_col("time_key"))
        .withColumn("time_bucket_15min", _calculate_time_bucket_col("time_key", 15))
        .withColumn("time_bucket_30min", _calculate_time_bucket_col("time_key", 30))
        .withColumn("time_bucket_hourly", _calculate_time_bucket_col("time_key", 60))
        .withColumn(
            "time_bucket_15min_str", _display_bucket_time_col("time_bucket_15min")
        )
        .withColumn(
            "time_bucket_30min_str", _display_bucket_time_col("time_bucket_30min")
        )
        .withColumn(
            "time_bucket_hourly_str", _display_bucket_time_col("time_bucket_hourly")
        )
        .withColumn("day_part", _calculate_day_part_col("time_key"))
        .drop("id")
    )

    time_dim_df = add_audit_columns(time_dim_df, batch_logical_date)
    time_dim_df = schema_enforcing(time_dim_df, TimeDimensionSchema)

    return time_dim_df
