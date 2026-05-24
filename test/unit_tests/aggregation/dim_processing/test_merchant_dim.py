import os
import sys

from test.utils.helpers import make_df, collect_col

from src.aws_pipeline.aggregation.dim_processing.merchant_dim import (
    _normalized_string_col,
    process_merchant_dim,
)
from src.aws_pipeline.schemas.gold_schema import MerchantDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


class TestNormalizedStringCol:
    """_normalized_string_col: trim strings and convert empty strings to null."""

    def test_null_empty_values(self, spark):
        df = make_df(spark, [" 5411 ", "", "   ", None])
        result = collect_col(df.withColumn("result", _normalized_string_col("value")))
        assert result == ["5411", None, None, None]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, [" Grocery Stores "])
        result = collect_col(df.withColumn("result", _normalized_string_col("value")))
        assert result == ["Grocery Stores"]


class TestProcessMerchantDim:
    def test_filtering_logic_and_duplicate_handling(self, spark):
        source_df = spark.createDataFrame(
            [
                (" 5411 ", " Grocery Stores "),
                ("5411", "Grocery Stores"),
                ("", "Blank code is rejected"),
                ("   ", "Whitespace code is rejected"),
                (None, "Null code is rejected"),
                ("5812", None),
            ],
            ["mcc_code", "merchant_name"],
        )

        result = (
            process_merchant_dim(source_df, "2024-01-04 00:00:00")
            .orderBy("merchant_key")
            .select("merchant_key", "merchant_category_code", "merchant_category")
            .collect()
        )

        assert [
            (
                row["merchant_key"],
                row["merchant_category_code"],
                row["merchant_category"],
            )
            for row in result
        ] == [
            (5411, "5411", "Grocery Stores"),
            (5812, "5812", None),
        ]

    def test_schema_contract_output(self, spark):
        source_df = spark.createDataFrame(
            [("5411", "Grocery Stores")],
            ["mcc_code", "merchant_name"],
        )

        result_df = process_merchant_dim(source_df, "2024-01-04 00:00:00")

        assert result_df.schema == MerchantDimensionSchema
        assert result_df.columns == MerchantDimensionSchema.fieldNames()
