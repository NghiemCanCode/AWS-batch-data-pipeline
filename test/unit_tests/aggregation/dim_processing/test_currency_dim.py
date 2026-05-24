import os
import sys

import pytest

from src.aws_pipeline.aggregation.dim_processing.currency_dim import (
    DEFAULT_CURRENCY_CODE,
    generate_currency_dim,
    _normalize_currency_codes,
)
from src.aws_pipeline.schemas.gold_schema import CurrencyDimensionSchema

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)


class TestNormalizeCurrencyCodes:
    """_normalize_currency_codes: default, trim, uppercase, dedup, and sort ISO codes."""

    def test_default_value_logic(self):
        assert _normalize_currency_codes(None) == [DEFAULT_CURRENCY_CODE]
        assert _normalize_currency_codes([]) == [DEFAULT_CURRENCY_CODE]
        assert _normalize_currency_codes([None, "", "   "]) == [DEFAULT_CURRENCY_CODE]

    def test_case_sensitivity_trim_dedup_sort(self):
        result = _normalize_currency_codes(
            [" eur ", "USD", "usd", None, " jpy", "", "EUR"]
        )

        assert result == ["EUR", "JPY", "USD"]


class TestGenerateCurrencyDim:
    def test_default_value_logic(self, spark):
        result = (
            generate_currency_dim(spark, "2024-01-04 00:00:00")
            .select("currency_key", "currency_type")
            .collect()
        )

        assert [(row["currency_key"], row["currency_type"]) for row in result] == [
            (840, DEFAULT_CURRENCY_CODE)
        ]

    def test_case_sensitivity_trim_dedup_sort(self, spark):
        result = (
            generate_currency_dim(
                spark,
                "2024-01-04 00:00:00",
                [" eur ", "USD", "usd", " jpy", "EUR"],
            )
            .orderBy("currency_type")
            .select("currency_key", "currency_type")
            .collect()
        )

        assert [(row["currency_key"], row["currency_type"]) for row in result] == [
            (978, "EUR"),
            (392, "JPY"),
            (840, "USD"),
        ]

    def test_invalid_enum_error_contract(self, spark):
        with pytest.raises(
            ValueError, match="Unsupported ISO 4217 currency code\\(s\\): BTC, USDD"
        ):
            generate_currency_dim(
                spark,
                "2024-01-04 00:00:00",
                ["usd", "btc", " USDD "],
            )

    def test_schema_output_contract(self, spark):
        result_df = generate_currency_dim(
            spark,
            "2024-01-04 00:00:00",
            ["vnd", " usd "],
        )

        assert result_df.schema == CurrencyDimensionSchema
        assert result_df.columns == CurrencyDimensionSchema.fieldNames()

        result = (
            result_df.orderBy("currency_type")
            .select(
                "currency_key",
                "currency_type",
                "_source_file",
                "_processing_id",
                "_batch_logical_date",
                "_is_deleted",
            )
            .collect()
        )

        assert [
            (
                row["currency_key"],
                row["currency_type"],
                row["_source_file"],
                row["_processing_id"] is not None,
                row["_batch_logical_date"].isoformat(sep=" "),
                row["_is_deleted"],
            )
            for row in result
        ] == [
            (840, "USD", "", True, "2024-01-04 00:00:00", False),
            (704, "VND", "", True, "2024-01-04 00:00:00", False),
        ]

        assert result_df.select("currency_type").distinct().count() == result_df.count()
