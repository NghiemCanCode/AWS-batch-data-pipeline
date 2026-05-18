from pyspark.sql import DataFrame, functions as F

from ...schemas.gold_schema import TransactionTypeDimensionSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


TRANSACTION_CHANNEL_COLUMN = "transaction_channel"
MAX_POSITIVE_SHORT = 32767


def _normalized_transaction_channel_col(column_name: str):
    trimmed_col = F.trim(F.col(column_name).cast("string"))
    return F.when(trimmed_col == F.lit(""), F.lit(None)).otherwise(trimmed_col)


def _transaction_type_key_col(column_name: str):
    # Hash modulo can collide; downstream loads should enforce the dimension key uniqueness contract.
    md5_prefix_as_long = F.conv(F.substring(F.md5(F.col(column_name)), 1, 7), 16, 10).cast("long")
    return (F.pmod(md5_prefix_as_long, F.lit(MAX_POSITIVE_SHORT)) + F.lit(1)).cast("short")


def process_transaction_type_dim(transactions_df: DataFrame, batch_logical_date) -> DataFrame:
    """
    Build the Transaction Type dimension from the Transactions silver DataFrame.
    """
    if TRANSACTION_CHANNEL_COLUMN not in transactions_df.columns:
        raise ValueError(
            f"Column '{TRANSACTION_CHANNEL_COLUMN}' is required to build Transaction Type dimension."
        )

    transaction_type_dim_df = (
        transactions_df.select(
            _normalized_transaction_channel_col(TRANSACTION_CHANNEL_COLUMN).alias("trans_type")
        )
        .where(F.col("trans_type").isNotNull())
        .distinct()
        .withColumn("trans_type_key", _transaction_type_key_col("trans_type"))
        .select("trans_type_key", "trans_type")
    )

    transaction_type_dim_df = add_audit_columns(transaction_type_dim_df, batch_logical_date)
    transaction_type_dim_df = schema_enforcing(
        transaction_type_dim_df,
        TransactionTypeDimensionSchema,
    )

    return transaction_type_dim_df
