import os
import sys

import pytest
from pyspark.sql import functions as F

from test.utils.helpers import make_df, collect_col

from src.aws_pipeline.aggregation.dim_processing.transaction_type_dim import (
    MAX_POSITIVE_SHORT,
    TRANSACTION_CHANNEL_COLUMN,
    _normalized_transaction_channel_col,
    _transaction_type_key_col,
    process_transaction_type_dim,
)
from src.aws_pipeline.schemas.gold_schema import TransactionTypeDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


class TestNormalizedTransactionChannelCol:
    """_normalized_transaction_channel_col: trim channels and convert empty strings to null."""

    def test_null_empty_values(self, spark):
        df = make_df(spark, [" Online ", "", "   ", None])
        result = collect_col(
            df.withColumn("result", _normalized_transaction_channel_col("value"))
        )
        assert result == ["Online", None, None, None]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, ["Swipe"])
        result = collect_col(
            df.withColumn("result", _normalized_transaction_channel_col("value"))
        )
        assert result == ["Swipe"]


class TestTransactionTypeKeyCol:
    """_transaction_type_key_col: stable positive short hash for a normalized channel."""

    def test_key_range_and_determinism(self, spark):
        df = make_df(spark, ["Online", "Online", "Chip"], "trans_type")
        result = (
            df.withColumn("result", _transaction_type_key_col("trans_type"))
            .select("trans_type", "result")
            .collect()
        )

        online_keys = [row["result"] for row in result if row["trans_type"] == "Online"]
        all_keys = [row["result"] for row in result]

        assert len(set(online_keys)) == 1
        assert all(1 <= key <= MAX_POSITIVE_SHORT for key in all_keys)


class TestProcessTransactionTypeDim:
    def test_filtering_logic_and_duplicate_handling(self, spark):
        source_df = spark.createDataFrame(
            [
                (" Online ",),
                ("Online",),
                ("",),
                ("   ",),
                (None,),
                ("Chip",),
            ],
            [TRANSACTION_CHANNEL_COLUMN],
        )

        result = (
            process_transaction_type_dim(source_df, "2024-01-04 00:00:00")
            .orderBy("trans_type")
            .select("trans_type_key", "trans_type")
            .collect()
        )
        expected_keys = {
            row["trans_type"]: row["trans_type_key"]
            for row in (
                source_df.select(
                    _normalized_transaction_channel_col(
                        TRANSACTION_CHANNEL_COLUMN
                    ).alias("trans_type")
                )
                .where(F.col("trans_type").isin("Chip", "Online"))
                .distinct()
                .withColumn("trans_type_key", _transaction_type_key_col("trans_type"))
                .collect()
            )
        }

        assert [(row["trans_type_key"], row["trans_type"]) for row in result] == [
            (expected_keys["Chip"], "Chip"),
            (expected_keys["Online"], "Online"),
        ]

    def test_missing_required_column_error(self, spark):
        source_df = spark.createDataFrame([("Online",)], ["channel"])

        with pytest.raises(ValueError, match=TRANSACTION_CHANNEL_COLUMN):
            process_transaction_type_dim(source_df, "2024-01-04 00:00:00")

    def test_schema_contract_output(self, spark):
        source_df = spark.createDataFrame(
            [("Online",)],
            [TRANSACTION_CHANNEL_COLUMN],
        )

        result_df = process_transaction_type_dim(
            source_df, "2024-01-04 00:00:00"
        )

        assert result_df.schema == TransactionTypeDimensionSchema
        assert result_df.columns == TransactionTypeDimensionSchema.fieldNames()
