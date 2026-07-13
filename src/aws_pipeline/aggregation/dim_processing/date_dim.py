"""
This dimension is generated periodically at specific times and does not run concurrently with the main pipelines.
"""

from datetime import datetime

import holidays
from pyspark.sql import SparkSession, DataFrame, Column, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampNTZType,
    DecimalType,
    IntegerType,
    DateType,
    BooleanType,
    ShortType,
)

DATE_FORMAT = "%Y-%m-%d"

def _calculate_is_weekend_col(column_name: str) -> Column:
    return F.when(
        (F.dayofweek(F.col(column_name)) == 1) | (F.dayofweek(F.col(column_name)) == 7),
        True,
    ).otherwise(False)


def _calculate_holiday_df(
    spark: SparkSession, start_year: int, end_year: int
) -> DataFrame:
    us_holidays = holidays.US(years=range(start_year, end_year + 1))
    holiday_list = [(date, name) for date, name in us_holidays.items()]

    return spark.createDataFrame(
        holiday_list, schema=["full_date", "holiday_name"]
    ).withColumn("full_date", F.to_date(F.col("full_date")))


def generate_date_dim(
    start_date: str, end_date: str, spark: SparkSession, batch_logical_date
) -> DataFrame:
    """
    Generate a date dimension table for a given date range.

    Business rule: date_dimension is a full-refresh reference table. It is not
    constrained by the 5-minute dashboard SLA, but Gold facts depend on its
    complete calendar coverage for partition and BI drill-down correctness.

    Args:
        start_date: The start date of the date range (YYYY-MM-DD).
        end_date: The end date of the date range (YYYY-MM-DD).

    Returns:
        A DataFrame containing the date dimension table.
    """
    try:
        parsed_start_date = datetime.strptime(start_date, DATE_FORMAT)
        parsed_end_date = datetime.strptime(end_date, DATE_FORMAT)
    except ValueError as e:
        raise ValueError("Invalid date format. Please use YYYY-MM-DD.") from e

    if parsed_start_date > parsed_end_date:
        raise ValueError("start_date must be less than or equal to end_date")

    start_year = parsed_start_date.year
    end_year = parsed_end_date.year
    start_date_value = parsed_start_date.strftime(DATE_FORMAT)
    end_date_value = parsed_end_date.strftime(DATE_FORMAT)

    holiday_df = _calculate_holiday_df(spark, start_year, end_year)

    date_dim_df = spark.range(1).select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date_value)),
                F.to_date(F.lit(end_date_value)),
                F.expr("interval 1 day"),
            )
        ).alias("full_date")
    )

    """
    SQL version of 

    SELECT
    explode(
        sequence(
            to_date('${start_date_value}'),
            to_date('${end_date_value}'),
            interval 1 day
        )
    ) AS full_date
    
    """

    date_dim_df = date_dim_df.withColumn(
        "date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int")
    )

    date_dim_df = (
        date_dim_df.withColumn(
            "day_of_week", F.dayofweek(F.col("full_date")).cast("short")
        )
        .withColumn("day_of_month", F.dayofmonth(F.col("full_date")).cast("short"))
        .withColumn("day_of_year", F.dayofyear(F.col("full_date")).cast("short"))
        .withColumn(
            "week_of_year", F.date_format(F.col("full_date"), "w").cast("short")
        )
        .withColumn("month", F.month(F.col("full_date")).cast("short"))
        .withColumn("quarter", F.quarter(F.col("full_date")).cast("short"))
        .withColumn("year", F.year(F.col("full_date")).cast("short"))
        .withColumn("is_weekend", _calculate_is_weekend_col("full_date"))
    )

    date_dim_df = date_dim_df.join(holiday_df, on="full_date", how="left")
    date_dim_df = date_dim_df.withColumn(
        "is_holiday", F.when(F.col("holiday_name").isNotNull(), True).otherwise(False)
    )

    return date_dim_df
