import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from aws_pipeline.transformation.load_data_source import read_bronze_csv, read_bronze_json


# ──────────────────────────────────────────────
# read_bronze_csv — input validation
# ──────────────────────────────────────────────

class TestReadBronzeCsvValidation:
    """Validates that read_bronze_csv raises ValueError for bad inputs."""

    def test_empty_base_path_raises(self):
        mock_spark = MagicMock()
        with pytest.raises(ValueError, match="non-empty"):
            read_bronze_csv(mock_spark, "", "bronze/data.csv")

    def test_empty_relative_path_raises(self):
        mock_spark = MagicMock()
        with pytest.raises(ValueError, match="non-empty"):
            read_bronze_csv(mock_spark, "s3://bucket", "")

    def test_none_base_path_raises(self):
        mock_spark = MagicMock()
        with pytest.raises((ValueError, TypeError)):
            read_bronze_csv(mock_spark, None, "bronze/data.csv")

    def test_none_relative_path_raises(self):
        mock_spark = MagicMock()
        with pytest.raises((ValueError, TypeError)):
            read_bronze_csv(mock_spark, "s3://bucket", None)


# ──────────────────────────────────────────────
# read_bronze_csv — Spark interaction
# ──────────────────────────────────────────────

class TestReadBronzeCsvSparkCalls:
    """Verifies that read_bronze_csv calls Spark read API correctly."""

    def test_default_options(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.option.return_value = mock_reader

        read_bronze_csv(mock_spark, "s3://bucket", "bronze/data.csv")

        # Default: header=True, inferSchema=False, delimiter=","
        mock_reader.option.assert_any_call("header", "true")
        mock_reader.option.assert_any_call("inferSchema", "false")
        mock_reader.option.assert_any_call("delimiter", ",")
        mock_reader.csv.assert_called_once_with("s3://bucket/bronze/data.csv")

    def test_custom_options(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.option.return_value = mock_reader

        read_bronze_csv(
            mock_spark, "s3://bucket", "bronze/data.csv",
            header=False, infer_schema=True, delimiter="|"
        )

        mock_reader.option.assert_any_call("header", "false")
        mock_reader.option.assert_any_call("inferSchema", "true")
        mock_reader.option.assert_any_call("delimiter", "|")

    def test_path_joining_strips_slashes(self):
        """Trailing slash on base + leading slash on relative should produce clean path."""
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_spark.read = mock_reader
        mock_reader.option.return_value = mock_reader

        read_bronze_csv(mock_spark, "s3://bucket/", "/bronze/data.csv")

        mock_reader.csv.assert_called_once_with("s3://bucket/bronze/data.csv")


# ──────────────────────────────────────────────
# read_bronze_json — input validation
# ──────────────────────────────────────────────

class TestReadBronzeJsonValidation:
    """Validates that read_bronze_json raises ValueError for bad inputs."""

    def test_empty_base_path_raises(self):
        mock_spark = MagicMock()
        with pytest.raises(ValueError, match="non-empty"):
            read_bronze_json(mock_spark, "", "bronze/data.json")

    def test_empty_relative_path_raises(self):
        mock_spark = MagicMock()
        with pytest.raises(ValueError, match="non-empty"):
            read_bronze_json(mock_spark, "s3://bucket", "")


# ──────────────────────────────────────────────
# read_bronze_json — Spark interaction
# ──────────────────────────────────────────────

class TestReadBronzeJsonSparkCalls:
    """Verifies that read_bronze_json calls Spark read API correctly."""

    def test_without_multiline(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.json.return_value = mock_df

        result = read_bronze_json(mock_spark, "s3://bucket", "bronze/mcc.json")

        mock_spark.read.json.assert_called_once_with("s3://bucket/bronze/mcc.json")
        assert result == mock_df

    def test_with_multiline(self):
        mock_spark = MagicMock()
        mock_reader = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.option.return_value = mock_reader
        mock_reader.json.return_value = mock_df

        result = read_bronze_json(
            mock_spark, "s3://bucket", "bronze/mcc.json",
            multiLine="true"
        )

        mock_spark.read.option.assert_called_once_with("multiLine", "true")
        mock_reader.json.assert_called_once_with("s3://bucket/bronze/mcc.json")
        assert result == mock_df

    def test_path_joining(self):
        mock_spark = MagicMock()
        mock_spark.read.json.return_value = MagicMock()

        read_bronze_json(mock_spark, "s3://bucket/", "/bronze/mcc.json")

        mock_spark.read.json.assert_called_once_with("s3://bucket/bronze/mcc.json")
