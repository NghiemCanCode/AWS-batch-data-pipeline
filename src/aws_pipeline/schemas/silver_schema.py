from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    TimestampType, 
    DecimalType, 
    IntegerType, 
    DoubleType
)

AUDIT_COLUMNS = [
    StructField("_ingested_at", TimestampType(), False),
    StructField("_source_file", StringType(), True),
    StructField("_processing_id", StringType(), False),
]

TransactionsSilverSchema = StructType([
    StructField("id", StringType(), False),
    StructField("date", TimestampType(), True),
    StructField("client_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("amount", DecimalType(10, 2), True),
    StructField("use_chip", StringType(), True),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("mcc", StringType(), True),
    StructField("errors", StringType(), True)
] + AUDIT_COLUMNS)

CardsSilverSchema = StructType([
    StructField("id", StringType(), False),
    StructField("client_id", StringType(), False),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("cvv", StringType(), True),
    StructField("has_chip", StringType(), True),
    StructField("num_cards_issued", IntegerType(), True),
    StructField("credit_limit", DecimalType(10, 2), True),
    StructField("acct_open_date", StringType(), True),
    StructField("year_pin_last_changed", IntegerType(), True),
    StructField("card_on_dark_web", StringType(), True)
]+ AUDIT_COLUMNS)

UsersSilverSchema = StructType([
    StructField("id", StringType(), False),
    StructField("current_age", IntegerType(), True),
    StructField("retirement_age", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("per_capita_income", DecimalType(10, 2), True),
    StructField("yearly_income", DecimalType(10, 2), True),
    StructField("total_debt", DecimalType(10, 2), True),
    StructField("credit_score", IntegerType(), True),
    StructField("num_credit_cards", IntegerType(), True)
]+ AUDIT_COLUMNS)

MccSilverSchema = StructType([
    StructField("mcc_code", StringType(), False),
    StructField("merchant_name", StringType(), True)
]+ AUDIT_COLUMNS)
