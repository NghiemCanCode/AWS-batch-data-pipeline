from pyspark.sql import DataFrame, functions as F

from ...schemas.gold_schema import AccountMonthlySnapshotFactSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


_REQUIRED_COLUMNS = (
    "date_key",
    "account_key",
    "customer_key",
    "currency_key",
    "transaction_amount",
    "trans_error_type_key",
)
_NON_ERROR_TRANS_ERROR_TYPE_KEY = 0


def _require_columns(df: DataFrame) -> None:
    missing_columns = [column for column in _REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            "account_transaction_fact_df missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def process_account_monthly_snapshot_fact(
    account_transaction_fact_df: DataFrame, batch_logical_date
) -> DataFrame:
    """
    Build monthly account snapshots from account transaction fact rows.

    Amounts greater than zero are treated as income. Amounts less than zero are
    treated as outcomes and reported as positive magnitudes.
    """
    _require_columns(account_transaction_fact_df)

    transaction_date = F.to_date(F.col("date_key").cast("string"), "yyyyMMdd")
    month_end_date = F.last_day(transaction_date)
    month_end_date_key = F.date_format(month_end_date, "yyyyMMdd").cast("int")
    days_in_month = F.dayofmonth(month_end_date)
    zero_amount = F.lit("0.00").cast("decimal(18,2)")

    monthly_totals = (
        account_transaction_fact_df.withColumn(
            "month_end_date_key", month_end_date_key
        )
        .withColumn("days_in_month", days_in_month)
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
        .groupBy(
            "month_end_date_key",
            "account_key",
            "customer_key",
            "currency_key",
        )
        .agg(
            F.count(F.lit(1)).cast("int").alias("transaction_count"),
            F.sum("income_amount").alias("total_income_amount"),
            F.sum("outcome_amount").alias("total_outcome_amount"),
            F.max("days_in_month").alias("days_in_month"),
            F.sum("successful_transaction")
            .cast("int")
            .alias("successful_transaction_count"),
        )
        .withColumn(
            "average_daily_total_income_amount",
            (F.col("total_income_amount") / F.col("days_in_month")).cast(
                "decimal(18,2)"
            ),
        )
        .withColumn(
            "average_daily_total_outcome_amount",
            (F.col("total_outcome_amount") / F.col("days_in_month")).cast(
                "decimal(18,2)"
            ),
        )
        .select(
            "month_end_date_key",
            "account_key",
            "customer_key",
            "currency_key",
            "transaction_count",
            "total_income_amount",
            "total_outcome_amount",
            "average_daily_total_income_amount",
            "average_daily_total_outcome_amount",
            "successful_transaction_count",
        )
    )

    monthly_totals = add_audit_columns(monthly_totals, batch_logical_date)
    return schema_enforcing(monthly_totals, AccountMonthlySnapshotFactSchema)
