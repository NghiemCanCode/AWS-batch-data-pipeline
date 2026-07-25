from pyspark.sql import DataFrame, functions as F


def _timestamp_ntz_lit(value):
    return F.lit(value).cast("timestamp_ntz")


def process_account_dim(cards_df: DataFrame, batch_logical_date) -> DataFrame:
    """
    Build the Account dimension from the Cards silver DataFrame.

    Business rule: Gold account/card attributes support 5-minute fraud
    dashboards and monthly account analytics. We use Type 2 history for card
    attributes that affect spend analysis or risk segmentation, while raw event
    history remains in Silver.
    """
    effective_from_date_col = _timestamp_ntz_lit(batch_logical_date)

    account_dim_df = (
        cards_df.withColumn("account_id", F.col("card_id"))
        .withColumn("effective_from_date", effective_from_date_col)
        .withColumn(
            "account_key",
            F.md5(
                F.concat_ws(
                    "||",
                    F.col("account_id").cast("string"),
                    F.col("effective_from_date").cast("string"),
                )
            ),
        )
        .withColumn("expires_month", F.month(F.col("expires")).cast("short"))
        .withColumn("expires_year", F.year(F.col("expires")).cast("short"))
        .withColumn("num_card_issue", F.col("num_cards_issued").cast("short"))
        .withColumn(
            "effective_to_date",
            F.lit("9999-12-31 23:59:59").cast("timestamp_ntz"),
        )
        .withColumn("is_current", F.lit(True))
        .withColumn("version_number", F.lit(1).cast("short"))
        .select(
            "account_key",
            "account_id",
            "card_type",
            "mask_card_number",
            "expires_month",
            "expires_year",
            "has_a_cvv",
            "has_chip",
            "num_card_issue",
            "effective_from_date",
            "effective_to_date",
            "is_current",
            "version_number",
        )
    )

    return account_dim_df
