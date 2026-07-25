from pyspark.sql import Column, DataFrame, functions as F


DEFAULT_CURRENCY_TYPE = "USD"
DEFAULT_TRANS_ERROR_TYPE = "No Error"


def _normalized_string_col(column_name: str) -> Column:
    trimmed_col = F.trim(F.col(column_name).cast("string"))
    return F.when(trimmed_col == F.lit(""), F.lit(None)).otherwise(trimmed_col)


def _normalized_join_col(column_name: str) -> Column:
    return F.upper(_normalized_string_col(column_name))


def _optional_normalized_join_col(df: DataFrame, column_names: tuple[str, ...]) -> Column:
    for column_name in column_names:
        if column_name in df.columns:
            return _normalized_join_col(column_name)

    return F.lit(None).cast("string")


def _currency_type_col(transactions_df: DataFrame) -> Column:
    currency_col = _optional_normalized_join_col(
        transactions_df,
        ("currency_type", "currency_code", "currency"),
    )
    return F.coalesce(currency_col, F.lit(DEFAULT_CURRENCY_TYPE))


def _trans_error_type_col(transactions_df: DataFrame) -> Column:
    if "trans_error_type" in transactions_df.columns:
        error_col = _normalized_string_col("trans_error_type")
    elif "error_type" in transactions_df.columns:
        error_col = _normalized_string_col("error_type")
    elif "errors" in transactions_df.columns:
        error_col = _normalized_string_col("errors_first_value")
    else:
        error_col = F.lit(None).cast("string")

    return F.coalesce(error_col, F.lit(DEFAULT_TRANS_ERROR_TYPE))


def _errors_first_value_col(transactions_df: DataFrame) -> Column:
    if "errors" not in transactions_df.columns:
        return F.lit(None).cast("string")

    return F.when(
        F.col("errors").isNotNull() & (F.size(F.col("errors")) > F.lit(0)),
        F.element_at(F.col("errors"), 1),
    )


def _date_key_col(timestamp_col: str) -> Column:
    return F.date_format(F.col(timestamp_col), "yyyyMMdd").cast("int")


def _time_key_col(timestamp_col: str) -> Column:
    return (
        F.hour(F.col(timestamp_col)) * F.lit(10000)
        + F.minute(F.col(timestamp_col)) * F.lit(100)
        + F.second(F.col(timestamp_col))
    ).cast("int")


def process_account_transaction_fact(
    transactions_df: DataFrame,
    customer_dim_df: DataFrame,
    account_dim_df: DataFrame,
    transaction_type_dim_df: DataFrame,
    merchant_dim_df: DataFrame,
    location_dim_df: DataFrame,
    trans_error_type_dim_df: DataFrame,
    currency_dim_df: DataFrame,
    batch_logical_date,
) -> DataFrame:
    """
    Build the transaction fact at one row per source transaction.

    Business rule: this dashboard-facing fact reports transactions against the
    current customer/account SCD2 versions available in Gold, not expensive
    point-in-time lookups. Dimension misses are preserved as NULL foreign keys
    and blocked by Audit 2, which avoids silently losing transactions while
    keeping the transform bounded for the 5-minute dashboard SLA.
    """
    transactions = (
        transactions_df.withColumn(
            "errors_first_value",
            _errors_first_value_col(transactions_df),
        )
        .withColumn("transaction_timestamp", F.col("timestamp").cast("timestamp_ntz"))
        .withColumn("date_key", _date_key_col("transaction_timestamp"))
        .withColumn("time_key", _time_key_col("transaction_timestamp"))
        .withColumn("customer_id_join", _normalized_join_col("client_id"))
        .withColumn("account_id_join", _normalized_join_col("card_id"))
        .withColumn("trans_type_join", _normalized_join_col("transaction_channel"))
        .withColumn("merchant_code_join", _normalized_join_col("mcc"))
        .withColumn("merchant_city_join", _normalized_join_col("merchant_city"))
        .withColumn("merchant_state_join", _normalized_join_col("merchant_state"))
        .withColumn(
            "trans_error_type_join",
            F.upper(_trans_error_type_col(transactions_df)),
        )
        .withColumn("currency_type_join", _currency_type_col(transactions_df))
        .alias("transactions")
    )

    customer_dim = (
        customer_dim_df.where(F.col("is_current").cast("boolean")).select(
            F.col("customer_key"),
            _normalized_join_col("customer_id").alias("customer_id_join"),
        )
        .alias("customer")
    )
    account_dim = (
        account_dim_df.where(F.col("is_current").cast("boolean")).select(
            F.col("account_key"),
            _normalized_join_col("account_id").alias("account_id_join"),
        )
        .alias("account")
    )
    transaction_type_dim = (
        transaction_type_dim_df.select(
            F.col("trans_type_key").alias("transaction_type_key"),
            _normalized_join_col("trans_type").alias("trans_type_join"),
        )
        .alias("transaction_type")
    )
    merchant_dim = (
        merchant_dim_df.select(
            F.col("merchant_key"),
            _normalized_join_col("merchant_category_code").alias("merchant_code_join"),
        )
        .alias("merchant")
    )
    location_dim = (
        location_dim_df.select(
            F.col("location_key").alias("merchant_location_key"),
            _normalized_join_col("city").alias("merchant_city_join"),
            _normalized_join_col("state").alias("merchant_state_join"),
        )
        .alias("location")
    )
    trans_error_type_dim = (
        trans_error_type_dim_df.select(
            F.col("trans_error_type_key"),
            _normalized_join_col("trans_error_type").alias("trans_error_type_join"),
        )
        .alias("trans_error_type")
    )
    currency_dim = (
        currency_dim_df.select(
            F.col("currency_key"),
            _normalized_join_col("currency_type").alias("currency_type_join"),
        )
        .alias("currency")
    )

    fact_df = (
        transactions.join(
            customer_dim,
            F.col("transactions.customer_id_join") == F.col("customer.customer_id_join"),
            "left",
        )
        .join(
            account_dim,
            F.col("transactions.account_id_join") == F.col("account.account_id_join"),
            "left",
        )
        .join(
            transaction_type_dim,
            F.col("transactions.trans_type_join")
            == F.col("transaction_type.trans_type_join"),
            "left",
        )
        .join(
            merchant_dim,
            F.col("transactions.merchant_code_join")
            == F.col("merchant.merchant_code_join"),
            "left",
        )
        .join(
            location_dim,
            (
                F.col("transactions.merchant_city_join")
                == F.col("location.merchant_city_join")
            )
            & (
                F.col("transactions.merchant_state_join")
                == F.col("location.merchant_state_join")
            ),
            "left",
        )
        .join(
            trans_error_type_dim,
            F.col("transactions.trans_error_type_join")
            == F.col("trans_error_type.trans_error_type_join"),
            "left",
        )
        .join(
            currency_dim,
            F.col("transactions.currency_type_join")
            == F.col("currency.currency_type_join"),
            "left",
        )
        .select(
            F.col("transactions.transaction_id").alias("transaction_id"),
            F.col("transactions.date_key").alias("date_key"),
            F.col("transactions.time_key").alias("time_key"),
            F.col("customer.customer_key").alias("customer_key"),
            F.col("account.account_key").alias("account_key"),
            F.col("transaction_type.transaction_type_key").alias("transaction_type_key"),
            F.col("merchant.merchant_key").alias("merchant_key"),
            F.col("location.merchant_location_key").alias("merchant_location_key"),
            F.col("trans_error_type.trans_error_type_key").alias("trans_error_type_key"),
            F.col("currency.currency_key").alias("currency_key"),
            F.col("transactions.amount").alias("transaction_amount"),
            F.col("transactions.zip").alias("zip_code"),
        )
    )

    return fact_df
