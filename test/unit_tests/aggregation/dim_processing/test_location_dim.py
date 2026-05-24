import hashlib
import sys
import os

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from test.utils.helpers import collect_col

from src.aws_pipeline.aggregation.dim_processing.location_dim import (
    process_location_dim,
    _normalized_string_col,
    _nullable_string_col,
    _select_location_pair,
    _location_key_col,
)
from src.aws_pipeline.schemas.gold_schema import LocationDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


def _md5(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _location_key(city: str | None, state: str | None) -> str:
    return _md5(f"{city or ''}||{state or ''}")


class TestNormalizedStringCol:
    """_normalized_string_col: casts to string, trims whitespace, and nulls empties."""

    def test_null_empty_values(self, spark):
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [(" Austin ",), ("",), ("   ",), (None,)],
            schema=schema,
        )

        result = collect_col(df.withColumn("result", _normalized_string_col("value")))

        assert result == ["Austin", None, None, None]


class TestNullableStringCol:
    """_nullable_string_col: returns normalized existing columns or typed nulls."""

    def test_existing_column_normalizes_value(self, spark):
        schema = StructType([StructField("city", StringType(), True)])
        df = spark.createDataFrame([(" Boston ",)], schema=schema)

        result = collect_col(df.withColumn("result", _nullable_string_col(df, "city")))

        assert result == ["Boston"]

    def test_missing_column_returns_null(self, spark):
        schema = StructType([StructField("city", StringType(), True)])
        df = spark.createDataFrame([("Boston",)], schema=schema)

        result = collect_col(df.withColumn("result", _nullable_string_col(df, "state")))

        assert result == [None]


class TestSelectLocationPair:
    """_select_location_pair: supports partial city/state pairs and rejects missing pairs."""

    def test_missing_state_column_keeps_city_with_null_state(self, spark):
        schema = StructType([StructField("city", StringType(), True)])
        df = spark.createDataFrame([(" Phoenix ",), ("   ",), (None,)], schema=schema)

        result_df = _select_location_pair(df, "city", "state")
        result = result_df.orderBy("city").collect()

        assert result_df.schema.fieldNames() == ["city", "state"]
        assert [(row["city"], row["state"]) for row in result] == [("Phoenix", None)]

    def test_missing_city_column_keeps_state_with_null_city(self, spark):
        schema = StructType([StructField("state", StringType(), True)])
        df = spark.createDataFrame([(" CA ",), ("",)], schema=schema)

        result_df = _select_location_pair(df, "city", "state")
        result = result_df.collect()

        assert [(row["city"], row["state"]) for row in result] == [(None, "CA")]

    def test_missing_both_columns_returns_none(self, spark):
        schema = StructType([StructField("postal_code", StringType(), True)])
        df = spark.createDataFrame([("94105",)], schema=schema)

        assert _select_location_pair(df, "city", "state") is None


class TestLocationKeyCol:
    def test_null_empty_values(self, spark):
        schema = StructType(
            [
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [("Austin", "TX"), ("Austin", None), (None, "TX"), (None, None)],
            schema=schema,
        )

        result = collect_col(df.withColumn("result", _location_key_col("city", "state")))

        assert result == [
            _location_key("Austin", "TX"),
            _location_key("Austin", None),
            _location_key(None, "TX"),
            _location_key(None, None),
        ]


class TestProcessLocationDim:
    def test_union_city_state_and_merchant_location_pairs(self, spark):
        schema = StructType(
            [
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("merchant_city", StringType(), True),
                StructField("merchant_state", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                (" Seattle ", " WA ", " Portland ", " OR "),
                ("Austin", "TX", "Austin", "TX"),
            ],
            schema=schema,
        )

        result_df = process_location_dim("2024-01-15 10:30:00", df)
        result = {
            (row["city"], row["state"], row["location_key"])
            for row in result_df.select("city", "state", "location_key").collect()
        }

        assert result_df.schema == LocationDimensionSchema
        assert result == {
            ("Seattle", "WA", _location_key("Seattle", "WA")),
            ("Portland", "OR", _location_key("Portland", "OR")),
            ("Austin", "TX", _location_key("Austin", "TX")),
        }

    def test_trim_empty_to_null_and_distinct_duplicates(self, spark):
        schema = StructType(
            [
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("merchant_city", StringType(), True),
                StructField("merchant_state", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                (" Austin ", " TX ", "Austin", "TX"),
                ("Austin", "TX", "   ", " CA "),
                ("", "", None, None),
            ],
            schema=schema,
        )

        result = (
            process_location_dim("2024-01-15 10:30:00", df)
            .select("city", "state", "location_key", "_batch_logical_date", "_is_deleted")
            .collect()
        )

        values = {(row["city"], row["state"], row["location_key"]) for row in result}
        assert values == {
            ("Austin", "TX", _location_key("Austin", "TX")),
            (None, "CA", _location_key(None, "CA")),
        }
        assert len(result) == 2
        assert {
            row["_batch_logical_date"].isoformat(sep=" ") for row in result
        } == {"2024-01-15 10:30:00"}
        assert {row["_is_deleted"] for row in result} == {False}

    def test_selects_supported_pairs_from_multiple_dataframes(self, spark):
        users_df = spark.createDataFrame(
            [("Denver", "CO")],
            schema=StructType(
                [
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                ]
            ),
        )
        transactions_df = spark.createDataFrame(
            [("Miami", "FL")],
            schema=StructType(
                [
                    StructField("merchant_city", StringType(), True),
                    StructField("merchant_state", StringType(), True),
                ]
            ),
        )

        result = {
            (row["city"], row["state"])
            for row in process_location_dim(
                "2024-01-15 10:30:00", users_df, transactions_df
            )
            .select("city", "state")
            .collect()
        }

        assert result == {("Denver", "CO"), ("Miami", "FL")}

    def test_requires_at_least_one_dataframe(self):
        with pytest.raises(
            ValueError,
            match="At least one source DataFrame is required",
        ):
            process_location_dim("2024-01-15 10:30:00")

    def test_requires_supported_location_columns(self, spark):
        schema = StructType([StructField("postal_code", StringType(), True)])
        df = spark.createDataFrame([("94105",)], schema=schema)

        with pytest.raises(
            ValueError,
            match="No supported location columns found",
        ):
            process_location_dim("2024-01-15 10:30:00", df)
