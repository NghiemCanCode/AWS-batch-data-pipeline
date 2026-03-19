"""
Unit tests for load_data_source.py

Tests the two Bronze-layer reader functions (CSV, JSON). Since these are
thin wrappers around Spark's read API, tests use mocks — no real Spark
session needed. Focus areas:
  - Input validation (empty/None paths rejected early)
  - Path construction (trailing/leading slashes normalized)
  - Spark options forwarded correctly (wrong options = silent data corruption)
  - Return value is the DataFrame from Spark
"""

import pytest
from unittest.mock import MagicMock
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from aws_pipeline.transformation.load_data_source import read_bronze_csv, read_bronze_json


# ── Helpers ───────────────────────────────────────────────────────────────────

def _csv_reader(mock_spark):
    """Set up a mock Spark CSV reader chain: spark.read.option(...).option(...).csv(...)"""
    mock_reader = MagicMock()
    mock_spark.read = mock_reader
    mock_reader.option.return_value = mock_reader
    return mock_reader


# ──────────────────────────────────────────────
# read_bronze_csv — input validation
# ──────────────────────────────────────────────

class TestReadBronzeCsvValidation:
    """Contract: both paths must be non-empty strings.
    Failing silently here points Spark at a wrong path — surfacing as an obscure runtime error.
    """

    @pytest.mark.parametrize("base, rel", [
        ("",            "bronze/data.csv"),
        ("s3://bucket", ""),
        ("",            ""),
    ])
    def test_empty_paths_raise_value_error(self, base, rel):
        with pytest.raises(ValueError, match="non-empty"):
            read_bronze_csv(MagicMock(), base, rel)

    def test_none_base_path_raises(self):
        with pytest.raises((ValueError, TypeError)):
            read_bronze_csv(MagicMock(), None, "bronze/data.csv")

    def test_none_relative_path_raises(self):
        with pytest.raises((ValueError, TypeError)):
            read_bronze_csv(MagicMock(), "s3://bucket", None)


# ──────────────────────────────────────────────
# read_bronze_csv — path construction
# ──────────────────────────────────────────────

class TestReadBronzeCsvPathConstruction:
    """Contract: composed path must be clean regardless of trailing/leading slash variations.
    Double slashes silently break S3 key lookups.
    """

    @pytest.mark.parametrize("base, rel, expected", [
        ("s3://bucket",   "bronze/data.csv",  "s3://bucket/bronze/data.csv"),
        ("s3://bucket/",  "bronze/data.csv",  "s3://bucket/bronze/data.csv"),
        ("s3://bucket",   "/bronze/data.csv", "s3://bucket/bronze/data.csv"),
        ("s3://bucket/",  "/bronze/data.csv", "s3://bucket/bronze/data.csv"),
        ("s3a://bucket",  "raw/file.csv",     "s3a://bucket/raw/file.csv"),
        ("hdfs:///data",  "bronze/file.csv",  "hdfs:///data/bronze/file.csv"),
    ])
    def test_path_normalization(self, base, rel, expected):
        mock_spark = MagicMock()
        mock_reader = _csv_reader(mock_spark)

        read_bronze_csv(mock_spark, base, rel)

        mock_reader.csv.assert_called_once_with(expected)


# ──────────────────────────────────────────────
# read_bronze_csv — Spark options
# ──────────────────────────────────────────────

class TestReadBronzeCsvSparkOptions:
    """Contract: Spark reader must receive exact options.
    Wrong options cause silent data corruption (header as data, wrong delimiter, etc).
    """

    def test_default_options(self):
        mock_spark = MagicMock()
        mock_reader = _csv_reader(mock_spark)

        read_bronze_csv(mock_spark, "s3://bucket", "data.csv")

        mock_reader.option.assert_any_call("header", "true")
        mock_reader.option.assert_any_call("inferSchema", "false")
        mock_reader.option.assert_any_call("delimiter", ",")

    @pytest.mark.parametrize("header, infer_schema, delimiter", [
        (False, True,  "|"),
        (True,  True,  "\t"),
        (False, False, ";"),
    ])
    def test_custom_options_forwarded(self, header, infer_schema, delimiter):
        mock_spark = MagicMock()
        mock_reader = _csv_reader(mock_spark)

        read_bronze_csv(
            mock_spark, "s3://bucket", "data.csv",
            header=header, infer_schema=infer_schema, delimiter=delimiter,
        )

        mock_reader.option.assert_any_call("header", str(header).lower())
        mock_reader.option.assert_any_call("inferSchema", str(infer_schema).lower())
        mock_reader.option.assert_any_call("delimiter", delimiter)

    def test_calls_csv_not_json(self):
        """Guard against copy-paste error calling the wrong reader format."""
        mock_spark = MagicMock()
        mock_reader = _csv_reader(mock_spark)

        read_bronze_csv(mock_spark, "s3://bucket", "data.csv")

        mock_reader.csv.assert_called_once()
        mock_reader.json.assert_not_called()

    def test_returns_dataframe_from_spark(self):
        mock_spark = MagicMock()
        mock_reader = _csv_reader(mock_spark)
        expected_df = MagicMock()
        mock_reader.csv.return_value = expected_df

        result = read_bronze_csv(mock_spark, "s3://bucket", "data.csv")

        assert result is expected_df


# ──────────────────────────────────────────────
# read_bronze_json — input validation
# ──────────────────────────────────────────────

class TestReadBronzeJsonValidation:
    """Same empty/None path contract as CSV."""

    @pytest.mark.parametrize("base, rel", [
        ("",            "bronze/mcc.json"),
        ("s3://bucket", ""),
        ("",            ""),
    ])
    def test_empty_paths_raise_value_error(self, base, rel):
        with pytest.raises(ValueError, match="non-empty"):
            read_bronze_json(MagicMock(), base, rel)

    def test_none_base_path_raises(self):
        with pytest.raises((ValueError, TypeError)):
            read_bronze_json(MagicMock(), None, "bronze/mcc.json")

    def test_none_relative_path_raises(self):
        with pytest.raises((ValueError, TypeError)):
            read_bronze_json(MagicMock(), "s3://bucket", None)


# ──────────────────────────────────────────────
# read_bronze_json — path construction
# ──────────────────────────────────────────────

class TestReadBronzeJsonPathConstruction:

    @pytest.mark.parametrize("base, rel, expected", [
        ("s3://bucket",   "bronze/mcc.json",  "s3://bucket/bronze/mcc.json"),
        ("s3://bucket/",  "bronze/mcc.json",  "s3://bucket/bronze/mcc.json"),
        ("s3://bucket",   "/bronze/mcc.json", "s3://bucket/bronze/mcc.json"),
        ("s3://bucket/",  "/bronze/mcc.json", "s3://bucket/bronze/mcc.json"),
    ])
    def test_path_normalization(self, base, rel, expected):
        mock_spark = MagicMock()
        mock_spark.read.json.return_value = MagicMock()

        read_bronze_json(mock_spark, base, rel)

        mock_spark.read.json.assert_called_once_with(expected)


# ──────────────────────────────────────────────
# read_bronze_json — Spark options
# ──────────────────────────────────────────────

class TestReadBronzeJsonSparkOptions:
    """Contract: ALL kwargs must reach Spark as reader options.
    Missing options cause silent parse failures (e.g. multiLine missing → malformed JSON as nulls).
    """

    def test_no_kwargs_calls_json_directly(self):
        mock_spark = MagicMock()
        expected_df = MagicMock()
        mock_spark.read.json.return_value = expected_df

        result = read_bronze_json(mock_spark, "s3://bucket", "bronze/mcc.json")

        mock_spark.read.json.assert_called_once_with("s3://bucket/bronze/mcc.json")
        assert result is expected_df

    def test_single_kwarg_forwarded(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        expected_df = MagicMock()
        mock_spark.read.option.return_value = mock_reader
        mock_reader.json.return_value = expected_df

        result = read_bronze_json(
            mock_spark, "s3://bucket", "bronze/mcc.json",
            multiLine="true",
        )

        mock_spark.read.option.assert_called_once_with("multiLine", "true")
        mock_reader.json.assert_called_once_with("s3://bucket/bronze/mcc.json")
        assert result is expected_df

    def test_multiple_kwargs_all_chained(self):
        """Every kwarg must reach Spark — missing one silently breaks JSON parsing."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_spark.read.option.return_value = mock_reader

        read_bronze_json(
            mock_spark, "s3://bucket", "bronze/mcc.json",
            multiLine="true", encoding="UTF-8", allowComments="true",
        )

        all_option_calls = (
            mock_spark.read.option.call_args_list
            + mock_reader.option.call_args_list
        )
        option_keys = [c.args[0] for c in all_option_calls]

        assert "multiLine" in option_keys
        assert "encoding" in option_keys
        assert "allowComments" in option_keys

    def test_calls_json_not_csv(self):
        """Guard against wrong reader format."""
        mock_spark = MagicMock()
        mock_spark.read.json.return_value = MagicMock()

        read_bronze_json(mock_spark, "s3://bucket", "data.json")

        mock_spark.read.json.assert_called_once()
        mock_spark.read.csv.assert_not_called()
