import os
import sys

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from test.utils.helpers import make_df, collect_col

from src.aws_pipeline.aggregation.dim_processing.trans_error_type_dim import (
    MAX_POSITIVE_SHORT,
    NO_ERROR_TYPE,
    NO_ERROR_TYPE_KEY,
    _normalized_error_type_col,
    _trans_error_type_key_col,
    process_trans_error_type_dim,
)
from src.aws_pipeline.schemas.gold_schema import TransErrorTypeDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


class TestNormalizedErrorTypeCol:
    """_normalized_error_type_col: trim errors and default null/empty values to No Error."""

    def test_null_empty_values(self, spark):
        df = make_df(spark, [" Insufficient Funds ", "", "   ", None])
        result = collect_col(df.withColumn("result", _normalized_error_type_col("value")))
        assert result == ["Insufficient Funds", NO_ERROR_TYPE, NO_ERROR_TYPE, NO_ERROR_TYPE]

    def test_representative_middle_values(self, spark):
        df = make_df(spark, ["Bad PIN"])
        result = collect_col(df.withColumn("result", _normalized_error_type_col("value")))
        assert result == ["Bad PIN"]


class TestTransErrorTypeKeyCol:
    """_trans_error_type_key_col: No Error uses key 0; other errors use positive short hashes."""

    def test_no_error_default_key_and_hashed_key_range(self, spark):
        df = make_df(spark, [NO_ERROR_TYPE, "Insufficient Funds"], "trans_error_type")
        result = (
            df.withColumn("result", _trans_error_type_key_col("trans_error_type"))
            .select("trans_error_type", "result")
            .collect()
        )
        key_by_type = {row["trans_error_type"]: row["result"] for row in result}

        assert key_by_type[NO_ERROR_TYPE] == NO_ERROR_TYPE_KEY
        assert 1 <= key_by_type["Insufficient Funds"] <= MAX_POSITIVE_SHORT


class TestProcessTransErrorTypeDim:
    def test_explode_no_error_default_and_duplicate_handling(self, spark):
        source_df = spark.createDataFrame(
            [
                ([" Insufficient Funds ", "Bad PIN", "Bad PIN"],),
                ([],),
                (None,),
                (["", "   ", None],),
            ],
            StructType([StructField("errors", ArrayType(StringType()), True)]),
        )

        result = (
            process_trans_error_type_dim(source_df, "2024-01-04 00:00:00")
            .orderBy("trans_error_type")
            .select("trans_error_type_key", "trans_error_type")
            .collect()
        )
        result_by_type = {
            row["trans_error_type"]: row["trans_error_type_key"] for row in result
        }

        assert sorted(result_by_type) == [
            "Bad PIN",
            "Insufficient Funds",
            NO_ERROR_TYPE,
        ]
        assert result_by_type[NO_ERROR_TYPE] == NO_ERROR_TYPE_KEY
        assert 1 <= result_by_type["Bad PIN"] <= MAX_POSITIVE_SHORT
        assert 1 <= result_by_type["Insufficient Funds"] <= MAX_POSITIVE_SHORT

    def test_schema_contract_output(self, spark):
        source_df = spark.createDataFrame(
            [(["Bad PIN"],)],
            StructType([StructField("errors", ArrayType(StringType()), True)]),
        )

        result_df = process_trans_error_type_dim(
            source_df, "2024-01-04 00:00:00"
        )

        assert result_df.schema == TransErrorTypeDimensionSchema
        assert result_df.columns == TransErrorTypeDimensionSchema.fieldNames()
