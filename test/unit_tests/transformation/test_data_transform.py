import pytest
import sys
import os
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from aws_pipeline.transformation.data_transform import clean_currency, mask_card_number


# ──────────────────────────────────────────────
# clean_currency
# ──────────────────────────────────────────────

class TestCleanCurrency:
    """Tests for clean_currency — removes '$' and ',' from a column."""

    def test_removes_dollar_and_comma(self, spark):
        data = [("$1,000.00",)]
        schema = StructType([StructField("amount", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = clean_currency(df, "amount").collect()

        assert result[0]["amount"] == "1000.00"

    def test_simple_value(self, spark):
        data = [("$50.50",)]
        schema = StructType([StructField("amount", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = clean_currency(df, "amount").collect()

        assert result[0]["amount"] == "50.50"

    def test_dash_is_preserved(self, spark):
        """clean_currency only removes $ and comma; '-' is NOT converted to None."""
        data = [("-",)]
        schema = StructType([StructField("amount", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = clean_currency(df, "amount").collect()

        # The function only does regexp_replace for [$,], so '-' stays as-is
        assert result[0]["amount"] == "-"

    def test_null_value(self, spark):
        data = [(None,)]
        schema = StructType([StructField("amount", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = clean_currency(df, "amount").collect()

        assert result[0]["amount"] is None

    def test_no_special_chars(self, spark):
        """Value without $ or comma should remain unchanged."""
        data = [("1234.56",)]
        schema = StructType([StructField("amount", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = clean_currency(df, "amount").collect()

        assert result[0]["amount"] == "1234.56"


# ──────────────────────────────────────────────
# mask_card_number
# ──────────────────────────────────────────────

class TestMaskCardNumber:
    """Tests for mask_card_number — masks all digits except the last 4."""

    def test_masks_all_but_last_four(self, spark):
        data = [("1234567890",)]
        schema = StructType([StructField("card_number", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = mask_card_number(df, "card_number").collect()

        assert result[0]["card_number"] == "******7890"

    def test_sixteen_digit_card(self, spark):
        data = [("4111111111111111",)]
        schema = StructType([StructField("card_number", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = mask_card_number(df, "card_number").collect()

        assert result[0]["card_number"] == "************1111"

    def test_exactly_four_digits(self, spark):
        """A 4-digit number should have zero mask characters."""
        data = [("7890",)]
        schema = StructType([StructField("card_number", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = mask_card_number(df, "card_number").collect()

        assert result[0]["card_number"] == "7890"

    def test_null_value(self, spark):
        data = [(None,)]
        schema = StructType([StructField("card_number", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result = mask_card_number(df, "card_number").collect()

        assert result[0]["card_number"] is None
