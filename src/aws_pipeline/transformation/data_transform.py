from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


# --- General ---
def trim_and_upper_col(column_name: str):
    return F.upper(F.trim(F.col(column_name).cast("string")))


def trim_and_lower_col(column_name: str):
    return F.lower(F.trim(F.col(column_name).cast("string")))


def clean_timestamp_col(column_name: str):
    return F.to_timestamp(F.trim(F.col(column_name)), "yyyy-MM-dd HH:mm:ss")


def clean_currency_col(column_name: str):
    return F.regexp_replace(F.col(column_name), r"[$,]", "").cast("decimal(18,2)")


def clean_bool_col(column_name: str):
    BOOL_LIST = ["TRUE", "FALSE"]

    cleaned_col = trim_and_upper_col(column_name)

    return F.when(
        cleaned_col.isin(BOOL_LIST), cleaned_col.cast(BooleanType())
    ).otherwise(F.lit(None).cast(BooleanType()))


def clean_num_col(column_name: str, num_range: list[int] = None):
    cleaned_col = F.trim(F.col(column_name))

    if num_range:
        num_1, num_2 = num_range[0], num_range[1]
        return F.when(
            cleaned_col.cast("int").between(num_1, num_2), cleaned_col.cast("int")
        ).otherwise(F.lit(None).cast("int"))
    else:
        return cleaned_col.cast("int")


def clean_category_col(column_name: str, category_list: list[str]):
    cleaned_col = trim_and_upper_col(column_name)
    return F.when(cleaned_col.isin(category_list), cleaned_col).otherwise(F.lit(None))


# --- Transaction ---


def clean_trans_errors_col(column_name: str):
    TRANS_ERROR_LIST = [
        "TECHNICAL GLITCH",
        "BAD EXPIRATION",
        "BAD CARD NUMBER",
        "INSUFFICIENT BALANCE",
        "BAD PIN",
        "BAD CVV",
        "BAD ZIPCODE",
    ]

    clean_str = trim_and_upper_col(column_name)
    data = F.split(clean_str, ",")

    transformed = F.transform(
        data, lambda x: F.when(x.isin(TRANS_ERROR_LIST), x).otherwise(F.lit("UNKNOWN"))
    )

    return F.when(F.length(clean_str) > 0, transformed).otherwise(F.lit(None))


# --- Card ---


def mask_card_num_col(column_name: str):
    """Masks all digits of the card number except the last 4."""
    card_num = F.col(column_name)

    return F.concat(
        F.repeat(F.lit("*"), F.length(card_num) - 4), F.substring(card_num, -4, 4)
    )


def clean_expires_col(column_name: str):
    return F.to_date(F.trim(F.col(column_name)), "MM/yy")


def clean_cvv_col(column_name: str):
    return F.when(
        F.col(column_name).cast("string").rlike(r"^[0-9]+$"), F.lit(True)
    ).otherwise(F.lit(False))


# --- User ---


def clean_address_col(column_name: str):
    return F.regexp_replace(F.col(column_name), r"^\d+\s+", "")
