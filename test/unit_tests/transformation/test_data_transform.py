"""
Unit tests for data_transform.py

Tests the correctness of individual column-level transform functions
used in the Bronze-to-Silver pipeline. Focus areas:
  - Null propagation (null-safe transforms never throw, always return null)
  - Return type correctness (matches Silver schema expectation)
  - Business rule validation (category whitelists, error code mapping)
  - Real-world CSV artifacts (whitespace, dollar signs, mixed case)
"""

import sys
import os
from datetime import date, datetime
from decimal import Decimal
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DecimalType,
    DoubleType,
)

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)

from aws_pipeline.transformation.data_transform import (
    trim_and_upper_col,
    trim_and_lower_col,
    clean_timestamp_col,
    clean_currency_col,
    clean_bool_col,
    clean_num_col,
    clean_category_col,
    clean_trans_errors_col,
    mask_card_num_col,
    clean_expires_col,
    clean_cvv_col,
    clean_address_col,
    reverse_geocode_address,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_df(spark, values: list, col_name: str = "value"):
    """Create a single-column DataFrame from a list of values (all StringType)."""
    return spark.createDataFrame(
        [(v,) for v in values],
        schema=StructType([StructField(col_name, StringType(), nullable=True)]),
    )


def collect_col(df, col_name: str = "result") -> list:
    """Collect all values of a single column into a Python list."""
    return [row[col_name] for row in df.collect()]


# ── trim_and_upper_col ────────────────────────────────────────────────────────


class TestTrimAndUpperCol:
    def test_strips_whitespace_and_uppercases(self, spark):
        df = make_df(spark, ["  hello world  "])
        result = collect_col(df.withColumn("result", trim_and_upper_col("value")))
        assert result == ["HELLO WORLD"]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", trim_and_upper_col("value")))
        assert result == [None]

    def test_empty_string_stays_empty(self, spark):
        df = make_df(spark, [""])
        result = collect_col(df.withColumn("result", trim_and_upper_col("value")))
        assert result == [""]


# ── trim_and_lower_col ────────────────────────────────────────────────────────


class TestTrimAndLowerCol:
    def test_strips_whitespace_and_lowercases(self, spark):
        df = make_df(spark, ["  HO CHI MINH  "])
        result = collect_col(df.withColumn("result", trim_and_lower_col("value")))
        assert result == ["ho chi minh"]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", trim_and_lower_col("value")))
        assert result == [None]


# ── clean_timestamp_col ───────────────────────────────────────────────────────


class TestCleanTimestampCol:
    def test_valid_timestamp_parsed_correctly(self, spark):
        df = make_df(spark, ["2026-03-15 10:30:00"])
        result = collect_col(df.withColumn("result", clean_timestamp_col("value")))
        assert result[0] == datetime(2026, 3, 15, 10, 30, 0)

    def test_invalid_format_returns_null(self, spark):
        df = make_df(spark, ["15/03/2026 10:30"])
        result = collect_col(df.withColumn("result", clean_timestamp_col("value")))
        assert result == [None]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_timestamp_col("value")))
        assert result == [None]

    def test_strips_whitespace_before_parse(self, spark):
        df = make_df(spark, ["  2026-03-15 10:30:00  "])
        result = collect_col(df.withColumn("result", clean_timestamp_col("value")))
        assert result[0] == datetime(2026, 3, 15, 10, 30, 0)


# ── clean_currency_col ────────────────────────────────────────────────────────


class TestCleanCurrencyCol:
    def test_removes_dollar_and_comma(self, spark):
        df = make_df(spark, ["$1,234.56"])
        result = collect_col(df.withColumn("result", clean_currency_col("value")))
        assert result == [Decimal("1234.56")]

    def test_plain_number(self, spark):
        df = make_df(spark, ["99.99"])
        result = collect_col(df.withColumn("result", clean_currency_col("value")))
        assert result == [Decimal("99.99")]

    def test_negative_value(self, spark):
        df = make_df(spark, ["-$50.00"])
        result = collect_col(df.withColumn("result", clean_currency_col("value")))
        assert result == [Decimal("-50.00")]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_currency_col("value")))
        assert result == [None]

    def test_non_numeric_returns_null(self, spark):
        df = make_df(spark, ["N/A"])
        result = collect_col(df.withColumn("result", clean_currency_col("value")))
        assert result == [None]

    def test_result_is_decimal_type(self, spark):
        """Ensures downstream schema_enforcing gets DecimalType, not StringType."""
        df = make_df(spark, ["100.00"])
        result_df = df.withColumn("result", clean_currency_col("value"))
        assert isinstance(result_df.schema["result"].dataType, DecimalType)


# ── clean_bool_col ────────────────────────────────────────────────────────────


class TestCleanBoolCol:
    def test_true_and_false_strings(self, spark):
        df = make_df(spark, ["TRUE", "FALSE"])
        result = collect_col(df.withColumn("result", clean_bool_col("value")))
        assert result == [True, False]

    def test_case_insensitive(self, spark):
        df = make_df(spark, ["true", "True", "FALSE", "False"])
        result = collect_col(df.withColumn("result", clean_bool_col("value")))
        assert result == [True, True, False, False]

    def test_whitespace_trimmed(self, spark):
        df = make_df(spark, ["  TRUE  ", "  false  "])
        result = collect_col(df.withColumn("result", clean_bool_col("value")))
        assert result == [True, False]

    def test_non_boolean_strings_return_null(self, spark):
        """YES/NO, 1/0 are not valid booleans — only TRUE/FALSE accepted."""
        df = make_df(spark, ["YES", "NO", "1", "0"])
        result = collect_col(df.withColumn("result", clean_bool_col("value")))
        assert result == [None, None, None, None]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_bool_col("value")))
        assert result == [None]

    def test_result_is_boolean_type(self, spark):
        """Both when/otherwise branches must cast to BooleanType for schema consistency."""
        df = make_df(spark, ["TRUE"])
        result_df = df.withColumn("result", clean_bool_col("value"))
        assert isinstance(result_df.schema["result"].dataType, BooleanType)


# ── clean_num_col ─────────────────────────────────────────────────────────────


class TestCleanNumCol:
    def test_valid_number_no_range(self, spark):
        df = make_df(spark, ["42"])
        result = collect_col(df.withColumn("result", clean_num_col("value")))
        assert result == [42]

    def test_whitespace_stripped(self, spark):
        df = make_df(spark, ["  99  "])
        result = collect_col(df.withColumn("result", clean_num_col("value")))
        assert result == [99]

    def test_within_range_returns_int(self, spark):
        df = make_df(spark, ["25"])
        result = collect_col(
            df.withColumn("result", clean_num_col("value", num_range=[1, 100]))
        )
        assert result == [25]

    def test_below_range_returns_null(self, spark):
        df = make_df(spark, ["0"])
        result = collect_col(
            df.withColumn("result", clean_num_col("value", num_range=[1, 100]))
        )
        assert result == [None]

    def test_above_range_returns_null(self, spark):
        df = make_df(spark, ["101"])
        result = collect_col(
            df.withColumn("result", clean_num_col("value", num_range=[1, 100]))
        )
        assert result == [None]

    def test_boundary_inclusive(self, spark):
        """between() is inclusive on both ends."""
        df = make_df(spark, ["1", "100"])
        result = collect_col(
            df.withColumn("result", clean_num_col("value", num_range=[1, 100]))
        )
        assert result == [1, 100]

    def test_non_numeric_returns_null(self, spark):
        df = make_df(spark, ["abc"])
        result = collect_col(df.withColumn("result", clean_num_col("value")))
        assert result == [None]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_num_col("value")))
        assert result == [None]

    def test_result_is_integer_type(self, spark):
        df = make_df(spark, ["42"])
        result_df = df.withColumn("result", clean_num_col("value", num_range=[1, 100]))
        assert isinstance(result_df.schema["result"].dataType, IntegerType)


# ── clean_category_col ────────────────────────────────────────────────────────


class TestCleanCategoryCol:
    CATEGORY_LIST = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]

    def test_valid_category(self, spark):
        df = make_df(spark, ["VISA"])
        result = collect_col(
            df.withColumn("result", clean_category_col("value", self.CATEGORY_LIST))
        )
        assert result == ["VISA"]

    def test_case_insensitive_matching(self, spark):
        df = make_df(spark, ["visa", "Mastercard"])
        result = collect_col(
            df.withColumn("result", clean_category_col("value", self.CATEGORY_LIST))
        )
        assert result == ["VISA", "MASTERCARD"]

    def test_whitespace_trimmed(self, spark):
        df = make_df(spark, ["  AMEX  "])
        result = collect_col(
            df.withColumn("result", clean_category_col("value", self.CATEGORY_LIST))
        )
        assert result == ["AMEX"]

    def test_unknown_value_returns_null(self, spark):
        df = make_df(spark, ["UNIONPAY", "JCB"])
        result = collect_col(
            df.withColumn("result", clean_category_col("value", self.CATEGORY_LIST))
        )
        assert result == [None, None]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(
            df.withColumn("result", clean_category_col("value", self.CATEGORY_LIST))
        )
        assert result == [None]


# ── clean_trans_errors_col ────────────────────────────────────────────────────


class TestCleanTransErrorsCol:
    def test_single_error(self, spark):
        df = make_df(spark, ["BAD PIN"])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result[0] == ["BAD PIN"]

    def test_multiple_errors(self, spark):
        df = make_df(spark, ["BAD PIN,BAD CVV"])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result[0] == ["BAD PIN", "BAD CVV"]

    def test_unknown_error_mapped_to_unknown(self, spark):
        df = make_df(spark, ["SOME WEIRD ERROR"])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result[0] == ["UNKNOWN"]

    def test_mix_valid_and_unknown(self, spark):
        df = make_df(spark, ["BAD PIN,WEIRD ERROR"])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result[0] == ["BAD PIN", "UNKNOWN"]

    def test_space_after_comma_not_trimmed_per_element(self, spark):
        """CSV artifacts: 'BAD PIN, BAD CVV' has a leading space on the second element.
        split(',') produces [' BAD CVV'] which won't match the whitelist after upper().
        This documents the current behavior — downstream must not rely on space-separated CSV errors.
        """
        df = make_df(spark, ["BAD PIN, BAD CVV"])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result[0] == ["BAD PIN", "UNKNOWN"]

    def test_case_insensitive(self, spark):
        df = make_df(spark, ["bad pin"])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result[0] == ["BAD PIN"]

    def test_empty_string_returns_null(self, spark):
        """No errors → NULL, not an empty array."""
        df = make_df(spark, [""])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result == [None]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_trans_errors_col("value")))
        assert result == [None]


# ── mask_card_num_col ─────────────────────────────────────────────────────────


class TestMaskCardNumCol:
    def test_masks_all_except_last_4(self, spark):
        df = make_df(spark, ["1234567890123456"])
        result = collect_col(df.withColumn("result", mask_card_num_col("value")))
        assert result == ["************3456"]

    def test_mask_length_matches_original(self, spark):
        original = "1234567890123456"
        df = make_df(spark, [original])
        result = collect_col(df.withColumn("result", mask_card_num_col("value")))
        assert len(result[0]) == len(original)

    def test_last_4_digits_preserved(self, spark):
        df = make_df(spark, ["9999888877776543"])
        result = collect_col(df.withColumn("result", mask_card_num_col("value")))
        assert result[0].endswith("6543")

    def test_masked_portion_is_asterisks(self, spark):
        df = make_df(spark, ["1234567890123456"])
        result = collect_col(df.withColumn("result", mask_card_num_col("value")))
        assert all(c == "*" for c in result[0][:-4])

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", mask_card_num_col("value")))
        assert result == [None]


# ── clean_expires_col ─────────────────────────────────────────────────────────


class TestCleanExpiresCol:
    def test_valid_expiry_parsed_to_first_of_month(self, spark):
        df = make_df(spark, ["03/2026"])
        result = collect_col(df.withColumn("result", clean_expires_col("value")))
        assert result[0] == date(2026, 3, 1)

    def test_invalid_format_returns_null(self, spark):
        df = make_df(spark, ["2026-03", "03-2026", "invalid"])
        result = collect_col(df.withColumn("result", clean_expires_col("value")))
        assert result == [None, None, None]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_expires_col("value")))
        assert result == [None]

    def test_whitespace_trimmed_before_parse(self, spark):
        df = make_df(spark, ["  03/2026  "])
        result = collect_col(df.withColumn("result", clean_expires_col("value")))
        assert result[0] == date(2026, 3, 1)


# ── clean_cvv_col ─────────────────────────────────────────────────────────────


class TestCleanCvvCol:
    def test_numeric_cvv_returns_true(self, spark):
        df = make_df(spark, ["123", "1234"])
        result = collect_col(df.withColumn("result", clean_cvv_col("value")))
        assert result == [True, True]

    def test_non_numeric_returns_false(self, spark):
        """has_a_cvv is a boolean flag — invalid CVV means False, not NULL."""
        df = make_df(spark, ["abc", "12a", ""])
        result = collect_col(df.withColumn("result", clean_cvv_col("value")))
        assert result == [False, False, False]

    def test_null_returns_false(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_cvv_col("value")))
        assert result == [False]


# ── clean_address_col ─────────────────────────────────────────────────────────


class TestCleanAddressCol:
    def test_removes_leading_street_number(self, spark):
        df = make_df(spark, ["123 Main Street"])
        result = collect_col(df.withColumn("result", clean_address_col("value")))
        assert result == ["Main Street"]

    def test_no_leading_number_unchanged(self, spark):
        df = make_df(spark, ["Main Street"])
        result = collect_col(df.withColumn("result", clean_address_col("value")))
        assert result == ["Main Street"]

    def test_multi_digit_number_removed(self, spark):
        df = make_df(spark, ["9999 Long Avenue"])
        result = collect_col(df.withColumn("result", clean_address_col("value")))
        assert result == ["Long Avenue"]

    def test_null_returns_null(self, spark):
        df = make_df(spark, [None])
        result = collect_col(df.withColumn("result", clean_address_col("value")))
        assert result == [None]

    def test_number_in_middle_preserved(self, spark):
        """Only the leading street number is stripped, not numbers within the address."""
        df = make_df(spark, ["123 Route 66 West"])
        result = collect_col(df.withColumn("result", clean_address_col("value")))
        assert result == ["Route 66 West"]


class TestReverseGeocodeAddress:
    coordinates = [
        (51.5214588, -0.1729636),
        (9.936033, 76.259952),
        (37.38605, -122.08385),
    ]

    def _make_coord_df(self, spark):
        schema = StructType(
            [
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ]
        )
        return spark.createDataFrame(self.coordinates, schema=schema)

    def test_returns_city_and_state_for_known_coordinates(self, spark):
        df = self._make_coord_df(spark)
        geo_schema = StructType(
            df.schema.fields
            + [
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
            ]
        )
        result_df = df.mapInPandas(
            reverse_geocode_address("latitude", "longitude"), schema=geo_schema
        )
        # key by (lat, lng) to avoid relying on partition order
        rows = {(r["latitude"], r["longitude"]): r for r in result_df.collect()}

        assert rows[(51.5214588, -0.1729636)]["city"] == "Bayswater"
        assert rows[(51.5214588, -0.1729636)]["state"] == "England"
        assert rows[(9.936033, 76.259952)]["city"] == "Cochin"
        assert rows[(9.936033, 76.259952)]["state"] == "Kerala"
        assert rows[(37.38605, -122.08385)]["city"] == "Mountain View"
        assert rows[(37.38605, -122.08385)]["state"] == "California"

    def test_output_schema_has_city_and_state_as_string(self, spark):
        df = self._make_coord_df(spark)
        geo_schema = StructType(
            df.schema.fields
            + [
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
            ]
        )
        result_df = df.mapInPandas(
            reverse_geocode_address("latitude", "longitude"), schema=geo_schema
        )
        assert isinstance(result_df.schema["city"].dataType, StringType)
        assert isinstance(result_df.schema["state"].dataType, StringType)
