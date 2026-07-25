import logging
import os
import sys

from pyspark.sql import SparkSession

try:
    from aws_pipeline.aggregation.silver_to_gold import (
        DATASETS,
        _run_dataset,
        process_silver_to_gold,
    )
except ImportError as e1:
    print(e1)

    try:
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        from src.aws_pipeline.aggregation.silver_to_gold import (
            DATASETS,
            _run_dataset,
            process_silver_to_gold,
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


def _optional_csv(key: str):
    value = os.environ.get(key)
    if not value:
        return None

    parsed_values = [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ]
    return parsed_values or None


def get_spark_session(app_name: str = "SilverToGold") -> SparkSession:
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
    batch_logical_date = _required("BATCH_LOGICAL_DATE")
    dataset_name = _optional("DATASET")  # None = run all, set by Airflow per task
    date_start = _optional("DATE_START")
    date_end = _optional("DATE_END")
    window_start = _optional("WINDOW_START")
    window_end = _optional("WINDOW_END")
    currencies = _optional_csv("CURRENCIES")

    spark = get_spark_session()

    logger.info("Starting Silver to Gold aggregation...")
    logger.info("Input Path:         %s", input_path)
    logger.info("Output Path:        %s", output_path)
    logger.info("Batch Logical Date: %s", batch_logical_date)
    logger.info("Dataset:            %s", dataset_name or "all")
    logger.info("Date Start:         %s", date_start or "infer")
    logger.info("Date End:           %s", date_end or "infer")
    logger.info("Window Start:       %s", window_start or "not set")
    logger.info("Window End:         %s", window_end or "not set")
    logger.info("Currencies:         %s", currencies or "default")

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
                batch_logical_date,
                options={
                    "date_start": date_start,
                    "date_end": date_end,
                    "window_start": window_start,
                    "window_end": window_end,
                    "currencies": currencies,
                },
            )
        else:
            process_silver_to_gold(
                spark,
                input_path,
                output_path,
                batch_logical_date,
                date_start=date_start,
                date_end=date_end,
                window_start=window_start,
                window_end=window_end,
                currencies=currencies,
            )

        logger.info("Job finished successfully.")
    except Exception as e:
        logger.error("Job failed: %s", e)
        sys.exit(1)
    finally:
        spark.stop()
