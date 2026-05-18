from pyspark.sql import DataFrame, functions as F

from ...schemas.gold_schema import TemporalTrendFactSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


_REQUIRED_COLUMNS = (
    "date_key",
    "time_key",
    "merchant_key",
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


def process_temporal_trend_fact(
    account_transaction_fact_df: DataFrame, batch_logical_date
) -> DataFrame:
    """
    Build temporal merchant trend aggregates from account transaction fact rows.

    Amounts greater than zero are treated as income. Amounts less than zero are
    treated as outcomes and reported as positive magnitudes.
    """
    _require_columns(account_transaction_fact_df)

    zero_amount = F.lit("0.00").cast("decimal(18,2)")

    temporal_trends = (
        account_transaction_fact_df.withColumn(
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
        .groupBy("date_key", "time_key", "merchant_key")
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
    )

    temporal_trends = add_audit_columns(temporal_trends, batch_logical_date)
    return schema_enforcing(temporal_trends, TemporalTrendFactSchema)
