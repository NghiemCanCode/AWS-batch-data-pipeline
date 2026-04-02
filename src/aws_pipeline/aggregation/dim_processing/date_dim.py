from datetime import datetime

import holidays
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from ...utils.audit_helpers import schema_enforcing, add_audit_columns
from ...schemas.gold_schema import DateDimensionSchema


def _calculate_week_of_month_col(column_name: str) -> F.col:
    return F.ceil(F.dayofmonth(F.col(column_name)) / 7).cast("short")


def _calculate_is_weekend_col(column_name: str) -> F.col:
    return F.when(
        (F.dayofweek(F.col(column_name)) == 1) | (F.dayofweek(F.col(column_name)) == 7),
        True,
    ).otherwise(False)


def _calculate_holy_day(spark, start_year, end_year):
    us_holidays = holidays.US(years=range(start_year, end_year + 1))
    holiday_list = [(date, name) for date, name in us_holidays.items()]

    holiday_df = spark.createDataFrame(
        holiday_list, schema=["full_date", "holiday_name"]
    )
    return holiday_df


def generate_date_dim(start_date: str, end_date: str, spark: SparkSession) -> DataFrame:
    """
    Generate a date dimension table for a given date range.

    Args:
        start_date: The start date of the date range (YYYY-MM-DD).
        end_date: The end date of the date range (YYYY-MM-DD).

    Returns:
        A DataFrame containing the date dimension table.
    """
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("Invalid date format. Please use YYYY-MM-DD.") from e

    if start_date > end_date:
        raise ValueError("start_date must be less than or equal to end_date")

    start_year = start_date.year
    end_year = end_date.year

    holiday_df = _calculate_holy_day(spark, start_year, end_year)

    date_dim_df = spark.range(1).select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date), "yyyy-MM-dd"),
                F.to_date(F.lit(end_date), "yyyy-MM-dd"),
                F.expr("interval 1 day"),
            )
        ).alias("full_date")
    )

    # TODO:String path vs Arithmetic path with date_key

    date_dim_df = date_dim_df.withColumn(
        "date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int")
    )

    date_dim_df = (
        date_dim_df.withColumn("day_of_week", F.dayofweek(F.col("full_date")))
        .withColumn("day_of_month", F.dayofmonth(F.col("full_date")))
        .withColumn("day_of_year", F.dayofyear(F.col("full_date")))
        .withColumn("week_of_month", _calculate_week_of_month_col("full_date"))
        .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
        .withColumn("month", F.month(F.col("full_date")))
        .withColumn("quarter", F.quarter(F.col("full_date")))
        .withColumn("year", F.year(F.col("full_date")))
        .withColumn("is_weekend", _calculate_is_weekend_col("full_date"))
    )

    date_dim_df = date_dim_df.join(holiday_df, on="full_date", how="left")
    date_dim_df = date_dim_df.withColumn(
        "is_holiday", F.when(F.col("holiday_name").isNotNull(), True).otherwise(False)
    )

    date_dim_df = add_audit_columns(date_dim_df)
    date_dim_df = schema_enforcing(date_dim_df, DateDimensionSchema)

    return date_dim_df
