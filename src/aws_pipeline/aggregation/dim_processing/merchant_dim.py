from pyspark.sql import DataFrame, functions as F

from ...schemas.gold_schema import MerchantDimensionSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


def _normalized_string_col(column_name: str) -> F.col:
    trimmed_col = F.trim(F.col(column_name).cast("string"))
    return F.when(trimmed_col == F.lit(""), F.lit(None)).otherwise(trimmed_col)


def process_merchant_dim(mcc_df: DataFrame, batch_logical_date) -> DataFrame:
    """
    Build the Merchant dimension from the MCC silver DataFrame.
    """
    merchant_dim_df = (
        mcc_df.select(
            _normalized_string_col("mcc_code").alias("merchant_category_code"),
            _normalized_string_col("merchant_name").alias("merchant_category"),
        )
        .where(F.col("merchant_category_code").isNotNull())
        .withColumn("merchant_key", F.col("merchant_category_code").cast("int"))
        .select(
            "merchant_key",
            "merchant_category_code",
            "merchant_category",
        )
        .distinct()
    )

    merchant_dim_df = add_audit_columns(merchant_dim_df, batch_logical_date)
    merchant_dim_df = schema_enforcing(merchant_dim_df, MerchantDimensionSchema)

    return merchant_dim_df
