from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampNTZType,
    DecimalType,
    IntegerType,
    DateType,
    BooleanType,
    ShortType,
)

# --- Data Governance Policies ---
# 1. Timezone: All Timestamp fields MUST be stored in UTC.
#    Conversion to local timezones (e.g., US/Eastern) should be handled at the BI/Reporting layer.
# 2. Surrogate Keys: MD5 Hashing is used for distributed scalability on Spark.
# 3. Data Privacy: Fields are tagged with [PII] or [SENSITIVE] for Governance.
#    Access to these fields should be restricted via Column-Level Security (e.g., Lake Formation).
TIMEZONE_POLICY = "UTC"

# --- System Audit Columns ---
SYSTEM_AUDIT_COLUMNS = [
    StructField("_created_at", TimestampNTZType(), False),  # UTC
    StructField("_source_file", StringType(), True),
    StructField("_processing_id", StringType(), False),
    StructField("_updated_at", TimestampNTZType(), False),  # UTC
    StructField("_batch_logical_date", TimestampNTZType(), False),  # UTC
    StructField("_is_deleted", BooleanType(), True),
]

# --- Dimensions ---

DateDimensionSchema = StructType(
    [
        # -- Primary key
        StructField("date_key", IntegerType(), False),  # Smart key
        # -- Dimension Attributes
        StructField("full_date", DateType(), False),
        StructField("day_of_week", ShortType(), True),
        StructField("day_of_month", ShortType(), True),
        StructField("day_of_year", ShortType(), True),
        StructField("week_of_month", ShortType(), True),
        StructField("week_of_year", ShortType(), True),
        StructField("month", ShortType(), True),
        StructField("quarter", ShortType(), True),
        StructField("year", ShortType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("is_holiday", BooleanType(), True),
        StructField("holiday_name", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

TimeDimensionSchema = StructType(
    [
        # -- Primary key
        StructField("time_key", IntegerType(), False),  # Smart key
        # -- Dimension Attributes
        StructField("time_24h", StringType(), True),
        StructField("hour_24", ShortType(), True),
        StructField("hour_12", ShortType(), True),
        StructField("am_pm", StringType(), True),
        StructField("minute", ShortType(), True),
        StructField("second", ShortType(), True),
        StructField("time_bucket_15min", ShortType(), True),
        StructField("time_bucket_30min", ShortType(), True),
        StructField("time_bucket_hourly", ShortType(), True),
        StructField("time_bucket_15min_str", StringType(), True),
        StructField("time_bucket_30min_str", StringType(), True),
        StructField("time_bucket_hourly_str", StringType(), True),
        StructField("day_part", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

CustomerDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Surrogate Key = MD5(customer_id + effective_from_date)
        StructField("customer_key", StringType(), False),
        # -- Dimension Attributes
        StructField(
            "customer_id", StringType(), False
        ),  # [PII] Natural Key from Source
        # StructField("retirement_age", ShortType(), True), Will be in BI Redshift's view
        # -- ML usecase, Will be limit BI Redshift's view
        StructField("birth_month", ShortType(), True),  # [SENSITIVE] Quasi-identifier
        StructField("birth_year", ShortType(), True),  # [SENSITIVE] Quasi-identifier
        StructField("gender", StringType(), True),  # [SENSITIVE] Quasi-identifier
        StructField(
            "income_bracket", StringType(), True
        ),  # [SENSITIVE] Financial profiling
        StructField(
            "address_key", StringType(), True
        ),  # Use location dimension as role-playing dimension
        # -- SCD Fields
        StructField("effective_from_date", TimestampNTZType(), False),  # UTC
        StructField("effective_to_date", TimestampNTZType(), False),  # UTC
        StructField("is_current", BooleanType(), False),
        StructField("version_number", ShortType(), False),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

AccountDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Surrogate Key = MD5(account_id + effective_from_date)
        StructField("account_key", StringType(), False),
        # -- Dimension Attributes
        StructField("account_id", StringType(), False),  # [PII] Natural Key from Source
        StructField("card_type", StringType(), False),
        StructField("mask_card_number", StringType(), False),  # [PII] Masked/Obfuscated
        StructField("expires_month", ShortType(), True),
        StructField("expires_year", ShortType(), True),
        # -- This field meant this card has a CVV, not a null value
        StructField("has_a_cvv", BooleanType(), True),
        StructField(
            "has_chip", BooleanType(), True
        ),  # -> ML usecase, Will be limit BI Redshift's view
        StructField(
            "num_card_issue", ShortType(), True
        ),  # -> ML usecase, Will be limit BI Redshift's view
        # -- SCD Fields
        StructField("effective_from_date", TimestampNTZType(), False),
        StructField("effective_to_date", TimestampNTZType(), False),
        StructField("is_current", BooleanType(), False),
        StructField("version_number", ShortType(), False),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

MerchantDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Static dimension, use natural key (MCC code)
        StructField("merchant_key", IntegerType(), False),
        # -- Dimension Attributes
        StructField("merchant_category_code", StringType(), True),
        StructField("merchant_category", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

LocationDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Surrogate Key = MD5(city + state + zip)
        StructField("location_key", StringType(), False),
        # -- Dimension Attributes
        StructField("city", StringType(), True),  # [PII] Location data
        StructField("state", StringType(), True),  # [PII] Location data
        StructField("zip", StringType(), True),  # [PII] Location data
    ]
    + SYSTEM_AUDIT_COLUMNS
)

TransactionTypeDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Static dimension, use natural key
        StructField("trans_type_key", ShortType(), False),
        # -- Dimension Attributes
        StructField("trans_type", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

TransErrorTypeDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Static dimension, use natural key
        StructField("trans_error_type_key", ShortType(), False),
        # -- Dimension Attributes
        StructField("trans_error_type", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

CurrencyDimensionSchema = StructType(
    [
        # -- Primary key
        # -- Static dimension, use natural key
        StructField("currency_key", ShortType(), False),
        # -- Dimension Attributes
        StructField("currency_type", StringType(), True),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

# --- Facts ---

AccountTransactionFactSchema = StructType(
    [
        # -- Primary key
        # -- Use base transaction_id from data source
        StructField("transaction_id", StringType(), False),
        # -- Foreign key
        StructField("date_key", IntegerType(), False),
        StructField("time_key", IntegerType(), False),
        StructField("customer_key", StringType(), False),
        StructField("account_key", StringType(), False),
        StructField("transaction_type_key", ShortType(), False),
        StructField("merchant_key", IntegerType(), False),
        StructField("merchant_location_key", StringType(), False),
        StructField("trans_error_type_key", ShortType(), False),
        StructField("currency_key", ShortType(), False),
        # -- Measures
        StructField("transaction_amount", DecimalType(18, 2), False),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

AccountMonthlySnapshotFactSchema = StructType(
    [
        # -- Foreign key
        StructField("month_end_date_key", IntegerType(), False),  # Smart key
        StructField("account_key", StringType(), False),
        StructField("customer_key", StringType(), False),
        StructField("currency_key", ShortType(), False),
        # -- Measures
        StructField("transaction_count", IntegerType(), False),
        StructField("total_income_amount", DecimalType(18, 2), False),
        StructField("total_outcome_amount", DecimalType(18, 2), False),
        StructField("average_daily_total_income_amount", DecimalType(18, 2), False),
        StructField("average_daily_total_outcome_amount", DecimalType(18, 2), False),
        StructField("successful_transaction_count", IntegerType(), False),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

TemporalTrendFactSchema = StructType(
    [
        # -- Foreign key
        StructField("date_key", IntegerType(), False),  # Smart key
        StructField("time_key", IntegerType(), False),  # Smart key
        StructField("merchant_key", IntegerType(), False),
        # -- Measures
        StructField("number_transaction", IntegerType(), False),
        StructField("successful_transaction_count", IntegerType(), False),
        StructField("total_income_amount", DecimalType(18, 2), False),
        StructField("total_outcome_amount", DecimalType(18, 2), False),
    ]
    + SYSTEM_AUDIT_COLUMNS
)

AccountOwnerFactlessSchema = StructType(
    [
        StructField("customer_key", StringType(), False),
        StructField("account_key", StringType(), False),
        StructField("valid_from_date", TimestampNTZType(), False),
        StructField("valid_to_date", TimestampNTZType(), False),
        StructField("is_active", BooleanType(), False),
    ]
    + SYSTEM_AUDIT_COLUMNS
)
