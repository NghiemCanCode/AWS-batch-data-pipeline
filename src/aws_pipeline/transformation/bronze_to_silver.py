from pyspark.sql.functions import (
    col, 
    regexp_replace, 
    current_timestamp, 
    input_file_name, 
    lit, 
    year,
    dayofmonth, 
    month, 
    create_map, 
    explode)
from itertools import chain
import os
import logging

# Ensure proper imports based on project structure
try:
    from aws_pipeline.schemas.silver_schema import (
        TransactionsSilverSchema, 
        CardsSilverSchema, 
        UsersSilverSchema,
        MccSilverSchema
    )
except ImportError:
    # Fallback for local testing if package not installed (though pyproject.toml compliant structure preferred)
    try:
        from src.aws_pipeline.schemas.silver_schema import (
            TransactionsSilverSchema, 
            CardsSilverSchema, 
            UsersSilverSchema,
            MccSilverSchema
        )
    except ImportError:
        print("Critical Error: Could not import schemas. Ensure 'aws_pipeline' is installed or in python path.")
        raise

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_bronze_csv(
    spark: "SparkSession",
    input_base_path: str,
    relative_path: str,
    *,
    header: bool = True,
    infer_schema: bool = False,
    delimiter: str = ",",
) -> "DataFrame":

    if not input_base_path or not relative_path:
        raise ValueError("Both 'input_base_path' and 'relative_path' must be non-empty strings.")

    # Use forward-slash joining — compatible with local FS, S3, and HDFS.
    input_path = f"{input_base_path.rstrip('/')}/{relative_path.lstrip('/')}"

    logger.info("Reading bronze CSV from %s", input_path)

    df = (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .option("delimiter", delimiter)
        .csv(input_path)
    )

    logger.info("Successfully read %d columns from %s", len(df.columns), input_path)
    return df

def read_bronze_json(
    spark: "SparkSession",
    input_base_path: str,
    relative_path: str,
) -> "DataFrame":

    if not input_base_path or not relative_path:
        raise ValueError("Both 'input_base_path' and 'relative_path' must be non-empty strings.")

    input_path = f"{input_base_path.rstrip('/')}/{relative_path.lstrip('/')}"

    logger.info("Reading bronze JSON from %s", input_path)

    df = spark.read.json(input_path)

    logger.info("Successfully read %d columns from %s", len(df.columns), input_path)
    return df

def clean_currency(df, column_name):
    """Removes '$' and ',' and converts to Decimal(10,2)."""
    df_cleaned = df.withColumn(
        column_name, 
        regexp_replace(col(column_name), "[$,]", "")
    )
    return df_cleaned

def apply_schema_casting(df, schema):
    """Casts DataFrame columns to match the target schema types."""
    select_cols = []
    for field in schema.fields:
        if field.name in df.columns:
            select_cols.append(col(field.name).cast(field.dataType))
        else:
            logger.warning(f"Column {field.name} missing from input dataframe. Inserting NULL.")
            select_cols.append(lit(None).cast(field.dataType).alias(field.name))
    return df.select(select_cols)

def transform_transactions(df):
    """applies transformations for transactions data"""
    logger.info("Transforming transactions data...")
    df_cleaned = clean_currency(df, "amount")
    
    # Cast schema first
    df_casted = apply_schema_casting(df_cleaned, TransactionsSilverSchema)
    
    # Add partition columns derived from 'date'
    return df_casted.withColumn("year", year(col("date"))) \
                    .withColumn("month", month(col("date"))) \
                    .withColumn("day", dayofmonth(col("date")))

def transform_cards(df):
    """applies transformations for cards data"""
    logger.info("Transforming cards data...")
    # Money columns: credit_limit
    df_cleaned = clean_currency(df, "credit_limit")
    return apply_schema_casting(df_cleaned, CardsSilverSchema)

def transform_users(df):
    """applies transformations for users data"""
    logger.info("Transforming users data...")
    # Money columns: per_capita_income, yearly_income, total_debt
    df_cleaned = clean_currency(df, "per_capita_income")
    df_cleaned = clean_currency(df_cleaned, "yearly_income")
    df_cleaned = clean_currency(df_cleaned, "total_debt")
    return apply_schema_casting(df_cleaned, UsersSilverSchema)

def transform_mcc(df: "DataFrame") -> "DataFrame":
    logger.info("Transforming mcc codes data...")

    # Build a Spark map literal: {col_name_1: value_1, col_name_2: value_2, ...}
    mapping = create_map(*list(chain.from_iterable(
        (lit(c), col(c).cast("string")) for c in df.columns
    )))

    # Derive alias names from schema (exclude audit columns starting with '_')
    schema_fields = [f.name for f in MccSilverSchema.fields if not f.name.startswith("_")]
    key_alias, value_alias = schema_fields[0], schema_fields[1]

    df_unpivoted = df.select(
        explode(mapping).alias(key_alias, value_alias)
    )

    return apply_schema_casting(df_unpivoted, MccSilverSchema)

def add_audit_columns(df):
    return df.withColumn("created_at", current_timestamp()) \
             .withColumn("source_file", input_file_name())

def process_bronze_to_silver(spark, input_base_path, output_base_path):
    """Orchestrates the bronze-to-silver transformation for all datasets."""

    DATASETS = {
        "transactions": {
            "input_path": "bronze/transactions_data.csv",
            "output_path": "silver/transactions",
            "transform_func": transform_transactions,
            "reader_type": "csv",
            "partition_by": ["year", "month", "day"],
        },
        "cards": {
            "input_path": "bronze/cards_data.csv",
            "output_path": "silver/cards",
            "transform_func": transform_cards,
            "reader_type": "csv",
        },
        "users": {
            "input_path": "bronze/users_data.csv",
            "output_path": "silver/users",
            "transform_func": transform_users,
            "reader_type": "csv",
        },
        "mcc": {
            "input_path": "bronze/mcc_codes.json",
            "output_path": "silver/mcc",
            "transform_func": transform_mcc,
            "reader_type": "json",
        },
    }

    for name, config in DATASETS.items():
        try:
            input_path = os.path.join(input_base_path, config["input_path"])
            logger.info("Reading %s from %s", name, input_path)

            # Choose the appropriate reader based on file type
            if config["reader_type"] == "json":
                raw_df = read_bronze_json(spark, input_base_path, config["input_path"])
            else:
                raw_df = read_bronze_csv(spark, input_base_path, config["input_path"])

            # Transform & add audit columns
            silver_df = config["transform_func"](raw_df)
            silver_df = add_audit_columns(silver_df)

            # Write to silver layer
            output_path = os.path.join(output_base_path, config["output_path"])
            writer = silver_df.write.mode("overwrite")

            partition_cols = config.get("partition_by")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer.parquet(output_path)
            logger.info("Successfully processed %s → %s", name, output_path)

        except Exception as e:
            logger.error("Error processing %s: %s", name, e)
            raise
