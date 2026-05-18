from pyspark.sql import DataFrame, functions as F

from ...schemas.gold_schema import CustomerDimensionSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


HIGH_EFFECTIVE_TO_DATE = "9999-12-31 23:59:59"


def _income_bracket_col(column_name: str) -> F.col:
    return (
        F.when(F.col(column_name) < F.lit(60000), F.lit("Low income"))
        .when(F.col(column_name) <= F.lit(180000), F.lit("Middle income"))
        .when(F.col(column_name).isNotNull(), F.lit("High income"))
    )


def _address_key_col(city_col: str, state_col: str) -> F.col:
    city = F.trim(F.col(city_col))
    state = F.trim(F.col(state_col))

    return F.when(
        (city != F.lit("")) & (state != F.lit("")),
        F.md5(F.concat(city, state)),
    )


def process_customer_dim(users_df: DataFrame, batch_logical_date) -> DataFrame:
    """
    Build the Customer dimension from the Users silver DataFrame.
    """
    effective_from_date = F.to_timestamp(F.lit(batch_logical_date))

    customer_dim_df = (
        users_df.withColumnRenamed("user_id", "customer_id")
        .withColumn("effective_from_date", effective_from_date)
        .withColumn(
            "customer_key",
            F.md5(
                F.concat(
                    F.col("customer_id"),
                    F.col("effective_from_date").cast("string"),
                )
            ),
        )
        .withColumn("income_bracket", _income_bracket_col("yearly_income"))
        .withColumn("address_key", _address_key_col("city", "state"))
        .withColumn("effective_to_date", F.to_timestamp(F.lit(HIGH_EFFECTIVE_TO_DATE)))
        .withColumn("is_current", F.lit(True))
        .withColumn("version_number", F.lit(1))
    )

    customer_dim_df = add_audit_columns(customer_dim_df, batch_logical_date)
    customer_dim_df = schema_enforcing(customer_dim_df, CustomerDimensionSchema)

    return customer_dim_df
