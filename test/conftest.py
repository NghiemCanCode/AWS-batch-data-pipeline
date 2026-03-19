import pytest
from pyspark.sql import SparkSession
import logging
import os

@pytest.fixture(scope="session")
def spark():
    """
    Creates a SparkSession for testing.
    """
    # Suppress logging during tests
    logging.disable(logging.CRITICAL)
    
    # Check for SPARK_REMOTE env var (e.g. sc://localhost:15002)
    spark_remote = os.environ.get("SPARK_REMOTE")

    if spark_remote:
        spark = SparkSession.builder \
            .remote(spark_remote) \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("PytestSparkSession") \
            .config("spark.sql.timestampType", "TIMESTAMP_NTZ") \
            .getOrCreate()
        
    yield spark
    
    spark.stop()
