import sys
import os
from pyspark.sql import SparkSession

# Assumes 'aws_pipeline' is installed in the environment or passed via --py-files
try:
    from aws_pipeline.transformation.bronze_to_silver import process_bronze_to_silver
except ImportError:
    # Fallback for local dev if src is in pythonpath
    try:
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        from src.aws_pipeline.transformation.bronze_to_silver import process_bronze_to_silver
    except ImportError:
        print("Error: 'aws_pipeline' module not found. Install it with 'pip install -e .' or pass --py-files.")
        sys.exit(1)

class ConfigError(Exception):
    pass

def _required(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise ConfigError(f"Missing required env var: {key}")
    return value

def _optional(key: str, default=None):
    return os.environ.get(key, default)

def get_spark_session(app_name="BronzeToSilver"):
    return SparkSession.builder.appName(app_name).getOrCreate()

if __name__ == "__main__":
    input_path = _required("INPUT_PATH")
    output_path = _required("OUTPUT_PATH")

    spark = get_spark_session()
    
    print(f"Starting Bronze to Silver transformation...")
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")
    
    try:
        process_bronze_to_silver(spark, input_path, output_path)
        print("Job finished successfully.")
    except Exception as e:
        print(f"Job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()
