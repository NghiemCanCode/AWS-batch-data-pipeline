from pyspark.sql.types import StructType, StructField, StringType
import sys
import os

# Ensure we can import from src if package not installed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from aws_pipeline.transformation.bronze_to_silver import (
    read_bronze_csv,
    read_bronze_json,
    clean_currency, 
    transform_users, 
    add_audit_columns, 
    transform_transactions, 
    transform_mcc,
    transform_cards    
)

def test_read_bronze_json(spark):
    data_path = ""

def test_clean_currency(spark):
    data = [("$1,000.00",), ("$50.50",), ("-",)]
    schema = StructType([StructField("amount", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    cleaned_df = clean_currency(df, "amount")
    
    results = cleaned_df.collect()
    
    assert results[0]["amount"] == "1000.00"
    assert results[1]["amount"] == "50.50"
    assert results[2]["amount"] is None
    
def test_transform_users_schema(spark):
    # Create minimal dummy data for users
    data = [
        ("1", "30", "65", "1990", "5", "Male", "123 St", "10.0", "20.0", "$50,000.00", "$100,000.00", "$0.00", "700", "2")
    ]
    columns = ["id", "current_age", "retirement_age", "birth_year", "birth_month", "gender", "address", 
                "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt", "credit_score", "num_credit_cards"]
    
    df = spark.createDataFrame(data, columns)
    transformed_df = transform_users(df)
    
    # Check explicit schema fields
    dtypes = dict(transformed_df.dtypes)
    assert dtypes['per_capita_income'] == 'decimal(10,2)'
    assert dtypes['current_age'] == 'int'
    
def test_audit_columns(spark):
    data = [("1",)]
    schema = StructType([StructField("id", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    result_df = add_audit_columns(df)
    
    assert "created_at" in result_df.columns
    assert "source_file" in result_df.columns

def test_transform_transactions_partitioning(spark):
    # Data with date
    data = [("1", "2023-10-27 10:00:00", "100", "200", "$50.00", "Chip", "500", "City", "State", "12345", "1234", "")]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("date", StringType(), True), # Input as string, cast to timestamp in schema
        StructField("client_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("amount", StringType(), True),
        StructField("use_chip", StringType(), True),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("errors", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    transformed_df = transform_transactions(df)
    
    columns = transformed_df.columns
    assert "year" in columns
    assert "month" in columns
    assert "day" in columns
    
    row = transformed_df.collect()[0]
    assert row["year"] == 2023
    assert row["month"] == 10
    assert row["day"] == 27

def test_transform_mcc(spark):
    # JSON structure simulated
    data = [("Eating Places", "Service Stations")]
    schema = StructType([
        StructField("5812", StringType(), True),
        StructField("5541", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    transformed_df = transform_mcc(df)
    
    # Expect mcc_code and description columns
    assert "mcc_code" in transformed_df.columns
    assert "description" in transformed_df.columns
    
    results = transformed_df.collect()
    # Should have 2 rows
    assert len(results) == 2
    
    # Verify content
    lookup = {row["mcc_code"]: row["description"] for row in results}
    assert lookup["5812"] == "Eating Places"
    assert lookup["5541"] == "Service Stations"
    assert lookup["5541"] == "Service Stations"

def test_transform_cards(spark):
    """Test cards transformation"""
    data = [
        ("1", "101", "Visa", "Credit", "1234567890", "12/25", "123", "Yes", "1", "$5,000.00", "2020-01-01", "2023", "No")
    ]
    columns = ["id", "client_id", "card_brand", "card_type", "card_number", "expires", "cvv", 
              "has_chip", "num_cards_issued", "credit_limit", "acct_open_date", 
              "year_pin_last_changed", "card_on_dark_web"]
    
    df = spark.createDataFrame(data, columns)
    
    transformed_df = transform_cards(df)
    
    # Check currency cleaning and casting
    row = transformed_df.collect()[0]
    assert row["credit_limit"] == 5000.00
    
    # Check schema types
    dtypes = dict(transformed_df.dtypes)
    assert dtypes["credit_limit"] == "decimal(10,2)"
    assert dtypes["num_cards_issued"] == "int"

def test_read_bronze_json():
    """Test read_bronze_json calls spark.read.json with correct path"""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.read.json.return_value = mock_df
    
    input_base = "s3://bucket"
    relative_path = "path/to/data.json"
    
    result = read_bronze_json(mock_spark, input_base, relative_path)
    
    expected_path = "s3://bucket/path/to/data.json"
    mock_spark.read.json.assert_called_once_with(expected_path)
    assert result == mock_df

def test_read_bronze_csv():
    """Test read_bronze_csv calls spark.read.csv with correct path and options"""
    mock_spark = MagicMock()
    mock_reader = MagicMock()
    mock_df = MagicMock()
    
    # Chain the mocks: spark.read -> .option() -> .option() -> .option() -> .csv()
    mock_spark.read = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.csv.return_value = mock_df
    
    input_base = "s3://bucket"
    relative_path = "path/to/data.csv"
    
    result = read_bronze_csv(mock_spark, input_base, relative_path, header=True, infer_schema=False, delimiter=",")
    
    expected_path = "s3://bucket/path/to/data.csv"
    
    # Verify option calls - order matters if strict, but 'any order' logic is better for robustness if we check specific calls
    # However, MagicMock records calls in order.
    # We expect:
    # .option("header", "true")
    # .option("inferSchema", "false")
    # .option("delimiter", ",")
    # .csv(path)
    
    mock_reader.option.assert_any_call("header", "true")
    mock_reader.option.assert_any_call("inferSchema", "false")
    mock_reader.option.assert_any_call("delimiter", ",")
    mock_reader.csv.assert_called_once_with(expected_path)
    
    assert result == mock_df
