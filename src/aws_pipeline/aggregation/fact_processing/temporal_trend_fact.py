from pyspark.sql import DataFrame, functions as F


_REQUIRED_COLUMNS = (
    "date_key",
    "time_key",
    "merchant_key",
    "transaction_amount",
    "trans_error_type_key",
)
_NON_ERROR_TRANS_ERROR_TYPE_KEY = 0


def _five_minute_time_key_col(column_name: str):
    time_key = F.col(column_name).cast("int")
    hour_part = F.floor(time_key / F.lit(10000)).cast("int")
    minute_part = F.floor((time_key % F.lit(10000)) / F.lit(100)).cast("int")
    bucket_minute = (F.floor(minute_part / F.lit(5)) * F.lit(5)).cast("int")
    return (hour_part * F.lit(10000) + bucket_minute * F.lit(100)).cast("int")


def _require_columns(df: DataFrame) -> None:
    missing_columns = [column for column in _REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            "account_transaction_fact_df missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def process_temporal_trend_fact(
    account_transaction_fact_df: DataFrame, batch_logical_date
) -> DataFrame:
    """
    Build temporal merchant trend aggregates from account transaction fact rows.

    Business rule: merchant trend dashboards refresh every 5 minutes and display
    one bucket per merchant per 5-minute interval. Seconds are rounded down to
    the bucket start to keep the aggregate aligned with the dashboard SLA.

    Amounts greater than zero are treated as income. Amounts less than zero are
    treated as outcomes and reported as positive magnitudes.
    """
    _require_columns(account_transaction_fact_df)

    zero_amount = F.lit("0.00").cast("decimal(18,2)")

    temporal_trends = (
        account_transaction_fact_df.withColumn(
            "trend_time_key",
            _five_minute_time_key_col("time_key"),
        )
        .withColumn(
            "income_amount",
            F.when(F.col("transaction_amount") > zero_amount, F.col("transaction_amount"))
            .otherwise(zero_amount)
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "outcome_amount",
            F.when(
                F.col("transaction_amount") < zero_amount,
                F.abs(F.col("transaction_amount")),
            )
            .otherwise(zero_amount)
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "successful_transaction",
            F.when(
                F.col("trans_error_type_key").cast("int")
                == F.lit(_NON_ERROR_TRANS_ERROR_TYPE_KEY),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .groupBy("date_key", "trend_time_key", "merchant_key")
        .agg(
            F.count(F.lit(1)).cast("int").alias("number_transaction"),
            F.sum("successful_transaction")
            .cast("int")
            .alias("successful_transaction_count"),
            F.sum("income_amount").cast("decimal(18,2)").alias("total_income_amount"),
            F.sum("outcome_amount")
            .cast("decimal(18,2)")
            .alias("total_outcome_amount"),
        )
        .withColumnRenamed("trend_time_key", "time_key")
    )

    return temporal_trends
