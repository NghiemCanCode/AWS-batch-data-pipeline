from pyspark.sql import DataFrame, functions as F

from ...schemas.gold_schema import AccountOwnerFactlessSchema
from ...utils.audit_helpers import add_audit_columns, schema_enforcing


_ACCOUNT_CUSTOMER_KEY_CANDIDATES = ("customer_key", "customer_id", "client_id", "user_id")
_ACCOUNT_ID_CANDIDATES = ("account_id", "card_id")


def _resolve_account_customer_column(account_dim_df: DataFrame) -> str:
    for column_name in _ACCOUNT_CUSTOMER_KEY_CANDIDATES:
        if column_name in account_dim_df.columns:
            return column_name

    raise ValueError(
        "account_dim_df must include one ownership column: "
        f"{', '.join(_ACCOUNT_CUSTOMER_KEY_CANDIDATES)}"
    )


def _resolve_account_id_column(df: DataFrame) -> str:
    for column_name in _ACCOUNT_ID_CANDIDATES:
        if column_name in df.columns:
            return column_name

    raise ValueError(
        "account ownership source must include one account id column: "
        f"{', '.join(_ACCOUNT_ID_CANDIDATES)}"
    )


def process_account_owner_factless(
    customer_dim_df: DataFrame,
    account_dim_df: DataFrame,
    batch_logical_date,
    account_owner_source_df: DataFrame | None = None,
) -> DataFrame:
    ownership_source_df = (
        account_owner_source_df if account_owner_source_df is not None else account_dim_df
    )
    account_customer_column = _resolve_account_customer_column(ownership_source_df)
    ownership_account_column = _resolve_account_id_column(ownership_source_df)
    customer_join_column = (
        "customer_key" if account_customer_column == "customer_key" else "customer_id"
    )

    if customer_join_column not in customer_dim_df.columns:
        raise ValueError(f"customer_dim_df must include '{customer_join_column}'")

    account = account_dim_df.alias("account")
    customer = customer_dim_df.alias("customer")
    ownership_source = ownership_source_df.alias("ownership_source")

    ownership_pairs = (
        ownership_source.join(
            account,
            F.col(f"ownership_source.{ownership_account_column}")
            == F.col("account.account_id"),
            "inner",
        )
        .join(
            customer,
            F.col(f"ownership_source.{account_customer_column}")
            == F.col(f"customer.{customer_join_column}"),
            "inner",
        )
        .where(
            (
                F.col("account.effective_from_date")
                <= F.col("customer.effective_to_date")
            )
            & (
                F.col("customer.effective_from_date")
                <= F.col("account.effective_to_date")
            )
        )
        .select(
            F.col("customer.customer_key").alias("customer_key"),
            F.col("account.account_key").alias("account_key"),
            F.greatest(
                F.col("customer.effective_from_date"),
                F.col("account.effective_from_date"),
            ).alias("valid_from_date"),
            F.least(
                F.col("customer.effective_to_date"),
                F.col("account.effective_to_date"),
            ).alias("valid_to_date"),
            (
                F.col("customer.is_current").cast("boolean")
                & F.col("account.is_current").cast("boolean")
            ).alias("is_active"),
        )
        .dropDuplicates(
            ["customer_key", "account_key", "valid_from_date", "valid_to_date"]
        )
    )

    ownership_pairs = add_audit_columns(ownership_pairs, batch_logical_date)
    return schema_enforcing(ownership_pairs, AccountOwnerFactlessSchema)
