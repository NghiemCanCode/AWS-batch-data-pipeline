import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType
import sys
import os

# Ensure we can import from src if package not installed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from aws_pipeline.transformation.bronze_to_silver import (
    apply_schema_casting,
    read_bronze_csv,
    read_bronze_json
)

def test_apply_schema_casting_basic(spark):
    """Test standard casting behavior"""
    data = [("1", "10.5", "100")]
    source_schema = ["id", "amount", "count"]
    df = spark.createDataFrame(data, source_schema)

    target_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("count", IntegerType(), True)
    ])

    result_df = apply_schema_casting(df, target_schema)
    
    # Check schema
    assert result_df.schema == target_schema
    
    # Check values
    row = result_df.collect()[0]
    assert row["id"] == 1
    assert row["amount"] == 10.50
    assert row["count"] == 100

def test_apply_schema_casting_missing_cols(spark):
    """Test handling of columns missing from input dataframe"""
    data = [("1",)]
    source_schema = ["id"] # Missing 'amount'
    df = spark.createDataFrame(data, source_schema)

    target_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("amount", DecimalType(10, 2), True)
    ])

    result_df = apply_schema_casting(df, target_schema)
    
    # Check schema has both columns
    assert "amount" in result_df.columns
    
    # Check values
    row = result_df.collect()[0]
    assert row["id"] == 1
    assert row["amount"] is None

def test_read_bronze_csv_validation(spark):
    """Test input validation for read_bronze_csv"""
    with pytest.raises(ValueError):
        read_bronze_csv(spark, "", "path")
    
    with pytest.raises(ValueError):
        read_bronze_csv(spark, "path", "")

def test_read_bronze_json_validation(spark):
    """Test input validation for read_bronze_json"""
    with pytest.raises(ValueError):
        read_bronze_json(spark, "", "path")
    
    with pytest.raises(ValueError):
        read_bronze_json(spark, "path", "")
