from pyspark.sql import Column, DataFrame, functions as F


NO_ERROR_TYPE = "No Error"
NO_ERROR_TYPE_KEY = 0
MAX_POSITIVE_SHORT = 32767


def _normalized_error_type_col(column_name: str) -> Column:
    trimmed_col = F.trim(F.col(column_name).cast("string"))
    return F.when(
        trimmed_col.isNull() | (trimmed_col == F.lit("")),
        F.lit(NO_ERROR_TYPE),
    ).otherwise(trimmed_col)


def _trans_error_type_key_col(column_name: str) -> Column:
    hashed_key = (
        (F.pmod(F.xxhash64(F.col(column_name)), F.lit(MAX_POSITIVE_SHORT)) + F.lit(1))
        .cast("short")
    )
    return F.when(
        F.col(column_name) == F.lit(NO_ERROR_TYPE),
        F.lit(NO_ERROR_TYPE_KEY),
    ).otherwise(hashed_key)


def process_trans_error_type_dim(
    transactions_df: DataFrame, batch_logical_date
) -> DataFrame:
    """
    Build the Transaction Error Type dimension from the Transactions silver DataFrame.

    Business rule: null or empty error arrays are mapped to the governed
    "No Error" member with key 0 so success metrics can join without special
    BI-side null handling.
    """
    errors_for_dimension = F.when(
        F.size(F.col("errors")) > F.lit(0),
        F.col("errors"),
    ).otherwise(F.array(F.lit(NO_ERROR_TYPE)))

    trans_error_type_dim_df = (
        transactions_df.select(F.explode(errors_for_dimension).alias("_raw_error_type"))
        .withColumn("trans_error_type", _normalized_error_type_col("_raw_error_type"))
        .withColumn(
            "trans_error_type_key",
            _trans_error_type_key_col("trans_error_type"),
        )
        .select("trans_error_type_key", "trans_error_type")
        .distinct()
    )

    return trans_error_type_dim_df
