import sys
import os
import logging
from pyspark.sql import SparkSession

try:
    from aws_pipeline.transformation.bronze_to_silver import (
        process_bronze_to_silver,
        _run_dataset,
        DATASETS,
    )
except ImportError as e1:
    print(e1)

    try:
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        from src.aws_pipeline.transformation.bronze_to_silver import (
            process_bronze_to_silver,
            _run_dataset,
            DATASETS,
        )
    except ImportError as e:
        print(e)
        sys.exit(1)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ConfigError(Exception):
    pass


def _required(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise ConfigError(f"Missing required env var: {key}")
    return value


def _optional(key: str, default=None):
    return os.environ.get(key, default)


def get_spark_session(app_name: str = "BronzeToSilver") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    input_path = _required("INPUT_PATH")
    output_path = _required("OUTPUT_PATH")
    quarantine_path = _required("QUARANTINE_PATH")
    batch_logical_date = _required("BATCH_LOGICAL_DATE")
    dataset_name = _optional("DATASET")  # None = run all, set by Airflow per task

    spark = get_spark_session()

    logger.info("Starting Bronze to Silver transformation...")
    logger.info("Input Path:      %s", input_path)
    logger.info("Output Path:     %s", output_path)
    logger.info("Quarantine Path: %s", quarantine_path)
    logger.info("Batch Logical Date: %s", batch_logical_date)
    logger.info("Dataset:         %s", dataset_name or "all")

    try:
        if dataset_name:
            if dataset_name not in DATASETS:
                logger.error(
                    "Unknown dataset: %s. Valid values: %s",
                    dataset_name,
                    list(DATASETS.keys()),
                )
                sys.exit(1)
            _run_dataset(
                spark,
                dataset_name,
                DATASETS[dataset_name],
                input_path,
                output_path,
                quarantine_path,
                batch_logical_date
            )
        else:
            # Local dev fallback — runs all datasets sequentially
            process_bronze_to_silver(spark, input_path, output_path, quarantine_path, batch_logical_date)

        logger.info("Job finished successfully.")
    except Exception as e:
        logger.error("Job failed: %s", e)
        sys.exit(1)
    finally:
        spark.stop()
