import pytest
from pyspark.sql import SparkSession
import logging

@pytest.fixture(scope="session")
def spark():
    """
    Creates a SparkSession for testing.
    """
    # Suppress logging during tests
    logging.disable(logging.CRITICAL)
    
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("PytestSparkSession") \
        .getOrCreate()
        
    yield spark
    
    spark.stop()
