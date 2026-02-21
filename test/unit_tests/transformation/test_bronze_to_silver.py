import pytest
import sys
import os
from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from aws_pipeline.transformation.bronze_to_silver import (
    apply_schema_casting,
    transform_transactions,
    transform_cards,
    transform_users,
    transform_mcc,
    add_audit_columns,
)
from aws_pipeline.schemas.silver_schema import (
    TransactionsSilverSchema,
    CardsSilverSchema,
    UsersSilverSchema,
    MccSilverSchema,
)


# ──────────────────────────────────────────────
# Helper: build a sample DataFrame for a schema
# ──────────────────────────────────────────────

def _string_schema(columns):
    """Create an all-StringType StructType (mimics raw CSV read)."""
    return StructType([StructField(c, StringType(), True) for c in columns])


# ──────────────────────────────────────────────
# apply_schema_casting
# ──────────────────────────────────────────────

class TestApplySchemaCasting:

    def test_basic_casting(self, spark):
        """Columns are cast to target types correctly."""
        data = [("1", "10.5", "100")]
        df = spark.createDataFrame(data, ["id", "amount", "count"])

        target = StructType([
            StructField("id", IntegerType(), True),
            StructField("amount", DecimalType(10, 2), True),
            StructField("count", IntegerType(), True),
        ])

        result = apply_schema_casting(df, target)

        assert result.schema == target
        row = result.collect()[0]
        assert row["id"] == 1
        assert row["amount"] == Decimal("10.50")
        assert row["count"] == 100

    def test_missing_column_filled_with_null(self, spark):
        """Columns absent from input are added as NULL with the correct type."""
        data = [("1",)]
        df = spark.createDataFrame(data, ["id"])

        target = StructType([
            StructField("id", IntegerType(), True),
            StructField("amount", DecimalType(10, 2), True),
        ])

        result = apply_schema_casting(df, target)

        assert "amount" in result.columns
        row = result.collect()[0]
        assert row["id"] == 1
        assert row["amount"] is None

    def test_only_selects_schema_columns(self, spark):
        """Extra columns NOT in the target schema are dropped."""
        data = [("1", "extra_value")]
        df = spark.createDataFrame(data, ["id", "extra_col"])

        target = StructType([
            StructField("id", IntegerType(), True),
        ])

        result = apply_schema_casting(df, target)

        assert result.columns == ["id"]
        assert "extra_col" not in result.columns


# ──────────────────────────────────────────────
# transform_transactions
# ──────────────────────────────────────────────

TRANSACTIONS_COLS = [
    "id", "date", "client_id", "card_id", "amount", "use_chip",
    "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors"
]


class TestTransformTransactions:

    def test_partition_columns_added(self, spark):
        """year, month, day columns are derived from date."""
        data = [("1", "2023-10-27 10:00:00", "100", "200", "$50.00",
                 "Chip", "500", "City", "State", "12345", "1234", "")]
        df = spark.createDataFrame(data, _string_schema(TRANSACTIONS_COLS))

        result = transform_transactions(df)

        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns

        row = result.collect()[0]
        assert row["year"] == 2023
        assert row["month"] == 10
        assert row["day"] == 27

    def test_amount_cleaned(self, spark):
        """Dollar signs and commas are removed from amount before casting."""
        data = [("1", "2023-01-01 00:00:00", "100", "200", "$1,234.56",
                 "Chip", "500", "City", "State", "12345", "1234", "")]
        df = spark.createDataFrame(data, _string_schema(TRANSACTIONS_COLS))

        result = transform_transactions(df)
        row = result.collect()[0]

        assert row["amount"] == Decimal("1234.56")

    def test_schema_matches_silver(self, spark):
        """Output schema (excluding partition cols) matches TransactionsSilverSchema fields."""
        data = [("1", "2023-01-01 00:00:00", "100", "200", "$50.00",
                 "Chip", "500", "City", "State", "12345", "1234", "")]
        df = spark.createDataFrame(data, _string_schema(TRANSACTIONS_COLS))

        result = transform_transactions(df)
        result_fields = {f.name for f in result.schema.fields}

        # All TransactionsSilverSchema fields should be present
        for field in TransactionsSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# transform_cards
# ──────────────────────────────────────────────

CARDS_COLS = [
    "id", "client_id", "card_brand", "card_type", "card_number", "expires",
    "cvv", "has_chip", "num_cards_issued", "credit_limit", "acct_open_date",
    "year_pin_last_changed", "card_on_dark_web"
]


class TestTransformCards:

    def test_credit_limit_cleaned_and_cast(self, spark):
        data = [("1", "101", "Visa", "Credit", "1234567890", "12/25",
                 "123", "Yes", "1", "$5,000.00", "2020-01-01", "2023", "No")]
        df = spark.createDataFrame(data, _string_schema(CARDS_COLS))

        result = transform_cards(df)
        row = result.collect()[0]

        assert row["credit_limit"] == Decimal("5000.00")

    def test_card_number_is_masked(self, spark):
        data = [("1", "101", "Visa", "Credit", "1234567890", "12/25",
                 "123", "Yes", "1", "$5,000.00", "2020-01-01", "2023", "No")]
        df = spark.createDataFrame(data, _string_schema(CARDS_COLS))

        result = transform_cards(df)
        row = result.collect()[0]

        # 10-digit card → 6 asterisks + last 4
        assert row["card_number"] == "******7890"

    def test_schema_types(self, spark):
        data = [("1", "101", "Visa", "Credit", "1234567890", "12/25",
                 "123", "Yes", "1", "$5,000.00", "2020-01-01", "2023", "No")]
        df = spark.createDataFrame(data, _string_schema(CARDS_COLS))

        result = transform_cards(df)
        dtypes = dict(result.dtypes)

        assert dtypes["credit_limit"] == "decimal(10,2)"
        assert dtypes["num_cards_issued"] == "int"
        assert dtypes["year_pin_last_changed"] == "int"

    def test_schema_matches_silver(self, spark):
        data = [("1", "101", "Visa", "Credit", "1234567890", "12/25",
                 "123", "Yes", "1", "$5,000.00", "2020-01-01", "2023", "No")]
        df = spark.createDataFrame(data, _string_schema(CARDS_COLS))

        result = transform_cards(df)
        result_fields = {f.name for f in result.schema.fields}

        for field in CardsSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# transform_users
# ──────────────────────────────────────────────

USERS_COLS = [
    "id", "current_age", "retirement_age", "birth_year", "birth_month",
    "gender", "address", "latitude", "longitude", "per_capita_income",
    "yearly_income", "total_debt", "credit_score", "num_credit_cards"
]


class TestTransformUsers:

    def test_currency_columns_cleaned(self, spark):
        data = [("1", "30", "65", "1990", "5", "Male", "123 St",
                 "10.0", "20.0", "$50,000.00", "$100,000.00", "$0.00", "700", "2")]
        df = spark.createDataFrame(data, _string_schema(USERS_COLS))

        result = transform_users(df)
        row = result.collect()[0]

        assert row["per_capita_income"] == Decimal("50000.00")
        assert row["yearly_income"] == Decimal("100000.00")
        assert row["total_debt"] == Decimal("0.00")

    def test_integer_casting(self, spark):
        data = [("1", "30", "65", "1990", "5", "Male", "123 St",
                 "10.0", "20.0", "$50,000.00", "$100,000.00", "$0.00", "700", "2")]
        df = spark.createDataFrame(data, _string_schema(USERS_COLS))

        result = transform_users(df)
        dtypes = dict(result.dtypes)

        assert dtypes["current_age"] == "int"
        assert dtypes["retirement_age"] == "int"
        assert dtypes["birth_year"] == "int"
        assert dtypes["credit_score"] == "int"

    def test_schema_matches_silver(self, spark):
        data = [("1", "30", "65", "1990", "5", "Male", "123 St",
                 "10.0", "20.0", "$50,000.00", "$100,000.00", "$0.00", "700", "2")]
        df = spark.createDataFrame(data, _string_schema(USERS_COLS))

        result = transform_users(df)
        result_fields = {f.name for f in result.schema.fields}

        for field in UsersSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# transform_mcc
# ──────────────────────────────────────────────

class TestTransformMcc:

    def test_unpivots_columns_to_rows(self, spark):
        """Each column becomes a row with (mcc_code, merchant_name)."""
        data = [("Eating Places", "Service Stations")]
        schema = StructType([
            StructField("5812", StringType(), True),
            StructField("5541", StringType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = transform_mcc(df)
        rows = result.collect()

        assert len(rows) == 2

        lookup = {row["mcc_code"]: row["merchant_name"] for row in rows}
        assert lookup["5812"] == "Eating Places"
        assert lookup["5541"] == "Service Stations"

    def test_output_has_correct_columns(self, spark):
        data = [("Eating Places",)]
        schema = StructType([StructField("5812", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = transform_mcc(df)

        assert "mcc_code" in result.columns
        assert "merchant_name" in result.columns

    def test_schema_matches_silver(self, spark):
        data = [("Eating Places",)]
        schema = StructType([StructField("5812", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = transform_mcc(df)
        result_fields = {f.name for f in result.schema.fields}

        for field in MccSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# add_audit_columns
# ──────────────────────────────────────────────

class TestAddAuditColumns:

    def test_adds_three_audit_columns(self, spark):
        data = [("1",)]
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = add_audit_columns(df)

        assert "_ingested_at" in result.columns
        assert "_source_file" in result.columns
        assert "_processing_id" in result.columns

    def test_original_columns_preserved(self, spark):
        data = [("1", "hello")]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame(data, schema)

        result = add_audit_columns(df)

        assert "id" in result.columns
        assert "name" in result.columns
        # Total = 2 original + 3 audit = 5
        assert len(result.columns) == 5

    def test_ingested_at_is_not_null(self, spark):
        data = [("1",)]
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = add_audit_columns(df)
        row = result.collect()[0]

        assert row["_ingested_at"] is not None

    def test_processing_id_is_not_null(self, spark):
        data = [("1",)]
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = add_audit_columns(df)
        row = result.collect()[0]

        assert row["_processing_id"] is not None
