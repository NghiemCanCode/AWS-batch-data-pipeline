from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampNTZType,
    DateType,
    DecimalType,
    IntegerType,
    ArrayType,
    BooleanType,
)

TIMEZONE_POLICY = "UTC"

# --- System Audit Columns ---

SYSTEM_AUDIT_COLUMNS = [
    StructField("_created_at", TimestampNTZType(), False),
    StructField("_source_file", StringType(), True),
    StructField("_processing_id", StringType(), False),
    StructField("_updated_at", TimestampNTZType(), False),
    StructField("_batch_logical_date", TimestampNTZType(), False),
    StructField("_is_deleted", BooleanType(), False),
]

TransactionsSilverSchema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", TimestampNTZType(), True),
        StructField("client_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("amount", DecimalType(18, 2), True),
        StructField("transaction_channel", StringType(), True),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("is_error", BooleanType(), False),
        StructField("errors", ArrayType(StringType()), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

CardsSilverSchema = StructType(
    [
        StructField("card_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("card_brand", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("mask_card_number", StringType(), True),
        StructField("expires", DateType(), True),
        StructField("has_a_cvv", BooleanType(), True),
        StructField("has_chip", BooleanType(), True),
        StructField("num_cards_issued", IntegerType(), True),
        StructField("credit_limit", DecimalType(18, 2), True),
        StructField("acct_open_date", DateType(), True),
        StructField("year_pin_last_changed", IntegerType(), True),
        StructField("card_on_dark_web", BooleanType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

UsersSilverSchema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("retirement_age", IntegerType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("birth_month", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("per_capita_income", DecimalType(18, 2), True),  # [PII]
        StructField("yearly_income", DecimalType(18, 2), True),  # [PII]
        StructField("total_debt", DecimalType(18, 2), True),
        StructField("credit_score", IntegerType(), True),  # [PII]
        StructField("num_credit_cards", IntegerType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

MccSilverSchema = StructType(
    [
        StructField("mcc_code", StringType(), False),
        StructField("merchant_name", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)
