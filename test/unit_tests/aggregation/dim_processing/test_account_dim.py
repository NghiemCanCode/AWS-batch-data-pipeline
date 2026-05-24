import hashlib
import os
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from test.utils.helpers import collect_col, make_df

from src.aws_pipeline.aggregation.dim_processing.account_dim import (
    _timestamp_ntz_lit,
    process_account_dim,
)
from src.aws_pipeline.schemas.gold_schema import AccountDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


CARDS_SCHEMA = StructType(
    [
        StructField("card_id", StringType(), False),
        StructField("card_type", StringType(), False),
        StructField("mask_card_number", StringType(), False),
        StructField("expires", DateType(), True),
        StructField("has_a_cvv", BooleanType(), True),
        StructField("has_chip", BooleanType(), True),
        StructField("num_cards_issued", IntegerType(), True),
    ]
)


def make_cards_df(spark, rows):
    return spark.createDataFrame(rows, schema=CARDS_SCHEMA)


def expected_account_key(account_id, batch_logical_date):
    return hashlib.md5(f"{account_id}{batch_logical_date}".encode()).hexdigest()


class TestTimestampNtzLit:
    """_timestamp_ntz_lit: casts literal values to Spark timestamp_ntz."""

    def test_representative_middle_values(self, spark):
        df = make_df(spark, ["x"])
        result = collect_col(
            df.withColumn(
                "result", _timestamp_ntz_lit("2024-05-24 12:34:56")
            )
        )
        assert result == [datetime(2024, 5, 24, 12, 34, 56)]

    def test_boundary_values(self, spark):
        df = make_df(spark, ["x"])
        result = collect_col(
            df.withColumn(
                "result", _timestamp_ntz_lit("9999-12-31 23:59:59")
            )
        )
        assert result == [datetime(9999, 12, 31, 23, 59, 59)]

    def test_null_empty_values(self, spark):
        df = make_df(spark, ["x"])
        result = collect_col(df.withColumn("result", _timestamp_ntz_lit(None)))
        assert result == [None]


class TestProcessAccountDim:
    def test_field_derivation_logic(self, spark):
        batch_logical_date = "2024-05-24 12:34:56"
        cards_df = make_cards_df(
            spark,
            [
                (
                    "card-001",
                    "Debit",
                    "************1111",
                    datetime(2026, 1, 31).date(),
                    True,
                    False,
                    2,
                )
            ],
        )

        result = (
            process_account_dim(cards_df, batch_logical_date)
            .select(
                "account_key",
                "account_id",
                "card_type",
                "mask_card_number",
                "expires_month",
                "expires_year",
                "has_a_cvv",
                "has_chip",
                "num_card_issue",
                "effective_from_date",
            )
            .collect()
        )

        assert [
            (
                row["account_key"],
                row["account_id"],
                row["card_type"],
                row["mask_card_number"],
                row["expires_month"],
                row["expires_year"],
                row["has_a_cvv"],
                row["has_chip"],
                row["num_card_issue"],
                row["effective_from_date"],
            )
            for row in result
        ] == [
            (
                expected_account_key("card-001", batch_logical_date),
                "card-001",
                "Debit",
                "************1111",
                1,
                2026,
                True,
                False,
                2,
                datetime(2024, 5, 24, 12, 34, 56),
            )
        ]

    def test_boundary_and_null_values(self, spark):
        batch_logical_date = "2024-01-01 00:00:00"
        cards_df = make_cards_df(
            spark,
            [
                (
                    "jan-card",
                    "Credit",
                    "************0001",
                    datetime(2024, 1, 1).date(),
                    None,
                    True,
                    None,
                ),
                (
                    "dec-card",
                    "Debit",
                    "************9999",
                    datetime(2024, 12, 31).date(),
                    False,
                    None,
                    32767,
                ),
            ],
        )

        result = (
            process_account_dim(cards_df, batch_logical_date)
            .orderBy("account_id")
            .select(
                "account_id",
                "expires_month",
                "expires_year",
                "has_a_cvv",
                "has_chip",
                "num_card_issue",
            )
            .collect()
        )

        assert [
            (
                row["account_id"],
                row["expires_month"],
                row["expires_year"],
                row["has_a_cvv"],
                row["has_chip"],
                row["num_card_issue"],
            )
            for row in result
        ] == [
            ("dec-card", 12, 2024, False, None, 32767),
            ("jan-card", 1, 2024, None, True, None),
        ]

    def test_schema_contract_output(self, spark):
        cards_df = make_cards_df(
            spark,
            [
                (
                    "card-001",
                    "Debit",
                    "************1111",
                    datetime(2026, 1, 31).date(),
                    True,
                    True,
                    1,
                )
            ],
        )

        result_df = process_account_dim(cards_df, "2024-05-24 12:34:56")

        assert result_df.schema == AccountDimensionSchema

    def test_scd_default_values(self, spark):
        batch_logical_date = "2024-05-24 12:34:56"
        cards_df = make_cards_df(
            spark,
            [
                (
                    "card-001",
                    "Debit",
                    "************1111",
                    datetime(2026, 1, 31).date(),
                    True,
                    True,
                    1,
                )
            ],
        )

        result = (
            process_account_dim(cards_df, batch_logical_date)
            .select(
                "effective_from_date",
                "effective_to_date",
                "is_current",
                "version_number",
                "_batch_logical_date",
                "_is_deleted",
            )
            .collect()
        )

        assert [
            (
                row["effective_from_date"],
                row["effective_to_date"],
                row["is_current"],
                row["version_number"],
                row["_batch_logical_date"],
                row["_is_deleted"],
            )
            for row in result
        ] == [
            (
                datetime(2024, 5, 24, 12, 34, 56),
                datetime(9999, 12, 31, 23, 59, 59),
                True,
                1,
                datetime(2024, 5, 24, 12, 34, 56),
                False,
            )
        ]

    def test_key_determinism(self, spark):
        batch_logical_date = "2024-05-24 12:34:56"
        cards_df = make_cards_df(
            spark,
            [
                (
                    "same-card",
                    "Debit",
                    "************1111",
                    datetime(2026, 1, 31).date(),
                    True,
                    True,
                    1,
                ),
                (
                    "same-card",
                    "Debit",
                    "************2222",
                    datetime(2027, 2, 28).date(),
                    False,
                    False,
                    2,
                ),
            ],
        )

        first_result = (
            process_account_dim(cards_df, batch_logical_date)
            .orderBy("mask_card_number")
            .select("account_key")
            .collect()
        )
        second_result = (
            process_account_dim(cards_df, batch_logical_date)
            .orderBy("mask_card_number")
            .select("account_key")
            .collect()
        )

        first_keys = [row["account_key"] for row in first_result]
        second_keys = [row["account_key"] for row in second_result]

        assert first_keys == second_keys
        assert first_keys == [
            expected_account_key("same-card", batch_logical_date),
            expected_account_key("same-card", batch_logical_date),
        ]

    def test_key_changes_when_effective_from_date_changes(self, spark):
        cards_df = make_cards_df(
            spark,
            [
                (
                    "card-001",
                    "Debit",
                    "************1111",
                    datetime(2026, 1, 31).date(),
                    True,
                    True,
                    1,
                )
            ],
        )

        first_key = collect_col(
            process_account_dim(cards_df, "2024-05-24 12:34:56").select(
                F.col("account_key").alias("result")
            )
        )
        second_key = collect_col(
            process_account_dim(cards_df, "2024-05-25 12:34:56").select(
                F.col("account_key").alias("result")
            )
        )

        assert first_key != second_key
