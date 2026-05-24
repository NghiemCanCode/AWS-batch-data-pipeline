import hashlib
import os
import sys

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from test.utils.helpers import collect_col

from src.aws_pipeline.aggregation.dim_processing.customer_dim import (
    HIGH_EFFECTIVE_TO_DATE,
    process_customer_dim,
    _address_key_col,
    _income_bracket_col,
)
from src.aws_pipeline.schemas.gold_schema import CustomerDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


def _md5(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


class TestIncomeBracketCol:
    """_income_bracket_col: maps yearly_income to low/middle/high income bands."""

    def test_boundary_values(self, spark):
        schema = StructType([StructField("yearly_income", IntegerType(), True)])
        df = spark.createDataFrame(
            [(59999,), (60000,), (180000,), (180001,), (None,)],
            schema=schema,
        )

        result = collect_col(
            df.withColumn("result", _income_bracket_col("yearly_income"))
        )

        assert result == [
            "Low income",
            "Middle income",
            "Middle income",
            "High income",
            None,
        ]


class TestAddressKeyCol:
    """_address_key_col: trims city/state and hashes only complete non-empty pairs."""

    def test_trim_null_empty_values(self, spark):
        schema = StructType(
            [
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                (" New York ", " NY "),
                ("", "CA"),
                ("   ", "CA"),
                (None, "CA"),
                ("Seattle", ""),
                ("Seattle", "  "),
                ("Seattle", None),
            ],
            schema=schema,
        )

        result = collect_col(df.withColumn("result", _address_key_col("city", "state")))

        assert result == [
            _md5("New YorkNY"),
            None,
            None,
            None,
            None,
            None,
            None,
        ]


class TestProcessCustomerDim:
    def test_schema_output_contract_and_scd_defaults(self, spark):
        schema = StructType(
            [
                StructField("user_id", StringType(), False),
                StructField("retirement_age", IntegerType(), True),
                StructField("birth_month", IntegerType(), True),
                StructField("birth_year", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("yearly_income", IntegerType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
            ]
        )
        users_df = spark.createDataFrame(
            [
                (
                    "user-1",
                    67,
                    4,
                    1980,
                    "F",
                    180001,
                    " Seattle ",
                    " WA ",
                )
            ],
            schema=schema,
        )

        result_df = process_customer_dim(users_df, "2024-01-15 10:30:00")

        assert result_df.schema == CustomerDimensionSchema

        row = result_df.select(
            "customer_key",
            "customer_id",
            "retirement_age",
            "birth_month",
            "birth_year",
            "gender",
            "income_bracket",
            "address_key",
            "effective_from_date",
            "effective_to_date",
            "is_current",
            "version_number",
            "_batch_logical_date",
            "_is_deleted",
        ).first()

        effective_from = row["effective_from_date"].isoformat(sep=" ")
        assert row["customer_key"] == _md5(f"user-1{effective_from}")
        assert row["customer_id"] == "user-1"
        assert row["retirement_age"] == 67
        assert row["birth_month"] == 4
        assert row["birth_year"] == 1980
        assert row["gender"] == "F"
        assert row["income_bracket"] == "High income"
        assert row["address_key"] == _md5("SeattleWA")
        assert effective_from == "2024-01-15 10:30:00"
        assert row["effective_to_date"].isoformat(sep=" ") == HIGH_EFFECTIVE_TO_DATE
        assert row["is_current"] is True
        assert row["version_number"] == 1
        assert row["_batch_logical_date"].isoformat(sep=" ") == "2024-01-15 10:30:00"
        assert row["_is_deleted"] is False
