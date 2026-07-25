"""
Bronze to Silver transformation job.

WRITE MODE PER DATASET
──────────────────────
  transactions : append
    - Each transaction is an immutable event and is never updated after ingestion.
    - audit_duplicates runs within the batch to catch duplicates from source re-sends.
    - IDEMPOTENCY (delegated to Airflow): if a job fails and retries, the same batch
      may be written twice, producing duplicate transaction_ids in Silver.
      Airflow must ensure each batch succeeds exactly once, or use a sensor
      to check _processing_id before triggering the job.

  cards, users : upsert
    - Mutable data — fields such as credit limit and address can change over time.
    - upsert_partition retains the latest record by timestamp and preserves _created_at.

  mcc : overwrite
    - Reference table replaced in full when a new version is available.
    - audit_duplicates and SCD tracking are not required.
"""

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
import logging

from ..utils.audit_helpers import schema_enforcing, add_audit_columns
from .load_data_source import read_bronze_csv, read_bronze_json
from .data_transform import (
    clean_category_col,
    clean_currency_col,
    clean_trans_errors_col,
    mask_card_num_col,
    clean_timestamp_col,
    trim_and_lower_col,
    trim_and_upper_col,
    clean_expires_col,
    clean_cvv_col,
    clean_bool_col,
    clean_num_col,
    clean_zip_col,
    reverse_geocode_address,
)

try:
    from aws_pipeline.schemas.silver_schema import (
        TransactionsSilverSchema,
        CardsSilverSchema,
        UsersSilverSchema,
        MccSilverSchema,
    )
except ImportError:
    try:
        from src.aws_pipeline.schemas.silver_schema import (
            TransactionsSilverSchema,
            CardsSilverSchema,
            UsersSilverSchema,
            MccSilverSchema,
        )
    except ImportError:
        print(
            "Critical Error: Could not import schemas. Ensure 'aws_pipeline' is installed or in python path."
        )
        raise

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TRANS_CHANNEL_LIST = ["SWIPE TRANSACTION", "ONLINE TRANSACTION", "CHIP TRANSACTION"]
CARD_BRAND_LIST = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
GENDER_LIST = ["MALE", "FEMALE"]


# --- AUDIT ---


def audit_mandatory_columns(
    df: DataFrame,
    mandatory_columns: list[str],
    source_name: str,
) -> tuple[DataFrame, DataFrame]:

    null_condition = F.lit(False)
    for c in mandatory_columns:
        null_condition = null_condition | F.col(c).isNull()

    null_cols_expr = F.concat_ws(
        ", ",
        *[F.when(F.col(c).isNull(), F.lit(c)) for c in mandatory_columns],
    )

    clean_df = df.filter(~null_condition)
    corrupted_df = (
        df.filter(null_condition)
        .withColumn("_quarantine_source", F.lit(source_name))
        .withColumn("_quarantine_reason", F.lit("null_mandatory_column"))
        .withColumn("_quarantine_null_cols", null_cols_expr)
        .withColumn("_quarantine_ts", F.current_timestamp())
    )

    clean_df.cache()
    corrupted_df.cache()

    return clean_df, corrupted_df


def audit_duplicates(
    df: DataFrame,
    id_col: str,
    timestamp_col: str,
    source_name: str,
) -> tuple[DataFrame, DataFrame]:

    from pyspark.sql.window import Window

    window = Window.partitionBy(id_col).orderBy(F.col(timestamp_col).desc())
    ranked = df.withColumn("_rn", F.row_number().over(window))

    clean_df = ranked.filter(F.col("_rn") == 1).drop("_rn")
    duplicate_df = ranked.filter(F.col("_rn") > 1).drop("_rn")

    quarantine_df = (
        duplicate_df.withColumn("_quarantine_source", F.lit(source_name))
        .withColumn("_quarantine_reason", F.lit("duplicate"))
        .withColumn("_quarantine_null_cols", F.lit(None).cast("string"))
        .withColumn("_quarantine_ts", F.current_timestamp())
    )

    clean_df.cache()
    quarantine_df.cache()

    return clean_df, quarantine_df


# --- WRITE ---


def upsert_partition(
    batch_df: DataFrame,
    silver_path: str,
    id_col: str,
    timestamp_col: str,
    partition_cols: list[str],
    target_schema: StructType,
    n_output_files: int | None = None,
) -> None:
    from pyspark.sql.window import Window

    spark = batch_df.sparkSession

    try:
        existing_silver = spark.read.parquet(silver_path)
        if partition_cols:
            affected_partitions_df = batch_df.select(*partition_cols).distinct()
            if affected_partitions_df.isEmpty():
                logger.warning("[UPSERT] No affected partitions found.")
                return
            silver_affected = existing_silver.join(
                affected_partitions_df, on=partition_cols, how="left_semi"
            )
        else:
            silver_affected = existing_silver
    except AnalysisException:
        # TODO: Catch Not found silver table, not all AnalysisException
        logger.warning("[UPSERT] Silver table not found, will create new.")
        silver_affected = spark.createDataFrame([], schema=target_schema)

    window = Window.partitionBy(id_col).orderBy(F.col(timestamp_col).desc())
    merged_df = (
        batch_df.unionByName(silver_affected)
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    writer = merged_df
    if n_output_files and partition_cols:
        writer = merged_df.repartition(n_output_files, *partition_cols)

    w = writer.write.mode("overwrite").format("parquet")
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.save(silver_path)

    if partition_cols:
        logger.info(
            f"[UPSERT] Overwrite {affected_partitions_df.count()} partition(s) → {silver_path}"
        )
    else:
        logger.info(f"[UPSERT] Overwrite (no partitions) → {silver_path}")


def write_quarantine(df: DataFrame, path: str) -> None:
    (
        df.withColumn("_quarantine_date", F.current_date())
        .write.mode("append")
        .partitionBy("_quarantine_source", "_quarantine_date")
        .format("parquet")
        .save(path)
    )


# --- TRANSFORM ---


def transform_transactions(df: DataFrame) -> DataFrame:
    logger.info("Transforming transactions data...")

    df = (
        df.withColumnRenamed("id", "transaction_id")
        .withColumn("timestamp", clean_timestamp_col("date"))
        .withColumn("amount", clean_currency_col("amount"))
        .withColumn(
            "transaction_channel", clean_category_col("use_chip", TRANS_CHANNEL_LIST)
        )
        .withColumn("merchant_city", trim_and_lower_col("merchant_city"))
        .withColumn("merchant_state", trim_and_upper_col("merchant_state"))
        .withColumn("zip", clean_zip_col("zip"))
        .withColumn("errors", clean_trans_errors_col("errors"))
        .withColumn("is_error", F.col("errors").isNotNull())
    )

    df = schema_enforcing(df, TransactionsSilverSchema)

    return (
        df.withColumn("year", F.year(F.col("timestamp")))
        .withColumn("month", F.month(F.col("timestamp")))
        .withColumn("day", F.dayofmonth(F.col("timestamp")))
    )


def transform_cards(df: DataFrame) -> DataFrame:
    logger.info("Transforming cards data...")

    df = (
        df.withColumnRenamed("id", "card_id")
        .withColumn("client_id", F.col("client_id").cast("string"))
        .withColumn("card_brand", clean_category_col("card_brand", CARD_BRAND_LIST))
        .withColumn("mask_card_number", mask_card_num_col("card_number"))
        .withColumn("expires", clean_expires_col("expires"))
        .withColumn("has_a_cvv", clean_cvv_col("cvv"))
        .withColumn("has_chip", clean_bool_col("has_chip"))
        .withColumn("credit_limit", clean_currency_col("credit_limit"))
        .withColumn("acct_open_date", clean_expires_col("acct_open_date"))
        .withColumn(
            "year_pin_last_changed",
            clean_num_col("year_pin_last_changed", num_range=[1990, 2026]),
        )
        .withColumn("card_on_dark_web", clean_bool_col("card_on_dark_web"))
    )

    return schema_enforcing(df, CardsSilverSchema)


def transform_users(df: DataFrame) -> DataFrame:
    logger.info("Transforming users data...")

    df = (
        df.withColumnRenamed("id", "user_id")
        .withColumn("user_id", F.col("user_id").cast("string"))
        .drop("current_age")
        .withColumn(
            "retirement_age", clean_num_col("retirement_age", num_range=[50, 100])
        )
        .withColumn("birth_year", clean_num_col("birth_year", num_range=[1900, 2026]))
        .withColumn("birth_month", clean_num_col("birth_month", num_range=[1, 12]))
        .withColumn("gender", clean_category_col("gender", GENDER_LIST))
        .withColumn("per_capita_income", clean_currency_col("per_capita_income"))
        .withColumn("yearly_income", clean_currency_col("yearly_income"))
        .withColumn("total_debt", clean_currency_col("total_debt"))
        .withColumn("credit_score", clean_num_col("credit_score", num_range=[300, 850]))
        .withColumn(
            "num_credit_cards", clean_num_col("num_credit_cards", num_range=[0, 100])
        )
    )

    geo_schema = StructType(
        df.schema.fields
        + [
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
        ]
    )
    df = df.mapInPandas(
        reverse_geocode_address("latitude", "longitude"), schema=geo_schema
    )

    return schema_enforcing(df, UsersSilverSchema)


def transform_mcc(df: DataFrame) -> DataFrame:
    logger.info("Transforming mcc codes data...")

    schema_fields = [
        f.name for f in MccSilverSchema.fields if not f.name.startswith("_")
    ]
    key_alias, value_alias = schema_fields[0], schema_fields[1]

    n = len(df.columns)
    stack_expr = (
        "stack({}, ".format(n)
        + ", ".join(
            "'{col}', cast(`{col}` as string)".format(col=c) for c in df.columns
        )
        + ")"
    )

    df_unpivoted = df.select(F.expr(stack_expr).alias(key_alias, value_alias))

    return schema_enforcing(df_unpivoted, MccSilverSchema)


_QUARANTINE_META_COLS = frozenset(
    {"_quarantine_source", "_quarantine_reason", "_quarantine_null_cols", "_quarantine_ts"}
)


def _stringify_quarantine(df: DataFrame) -> DataFrame:
    return df.select([
        F.col(c) if c in _QUARANTINE_META_COLS else F.col(c).cast("string").alias(c)
        for c in df.columns
    ])


DATASETS = {
    "transactions": {
        "input_path": "bronze/transactions_data.csv",
        "output_path": "silver/transactions",
        "quarantine_path": "quarantine/transactions",
        "transform_func": transform_transactions,
        "reader_type": "csv",
        "write_mode": "append",
        "partition_cols": ["year", "month", "day"],
        "mandatory_cols": ["id", "client_id", "card_id", "amount", "merchant_id"],
        "silver_schema": TransactionsSilverSchema,
    },
    "cards": {
        "input_path": "bronze/cards_data.csv",
        "output_path": "silver/cards",
        "quarantine_path": "quarantine/cards",
        "transform_func": transform_cards,
        "reader_type": "csv",
        "write_mode": "upsert",
        "partition_cols": [],
        "mandatory_cols": ["id", "client_id", "card_number"],
        "id_col": "card_id",
        "timestamp_col": "acct_open_date",
        "silver_schema": CardsSilverSchema,
    },
    "users": {
        "input_path": "bronze/users_data.csv",
        "output_path": "silver/users",
        "quarantine_path": "quarantine/users",
        "transform_func": transform_users,
        "reader_type": "csv",
        "write_mode": "upsert",
        "partition_cols": [],
        "mandatory_cols": ["id", "longitude", "latitude"],
        "id_col": "user_id",
        "timestamp_col": "_updated_at",
        "silver_schema": UsersSilverSchema,
    },
    "mcc": {
        "input_path": "bronze/mcc_codes.json",
        "output_path": "silver/mcc",
        "quarantine_path": "quarantine/mcc",
        "transform_func": transform_mcc,
        "reader_type": "json",
        "reader_options": {"multiLine": "true"},
        "write_mode": "overwrite",
        "partition_cols": [],
        "mandatory_cols": [],
        "silver_schema": MccSilverSchema,
    },
}


def _write_dataset(clean_df: DataFrame, output_path: str, config: dict) -> None:
    """Routes write to the correct strategy based on write_mode."""
    write_mode = config["write_mode"]
    partition_cols = config.get("partition_cols", [])

    if write_mode == "append":
        writer = clean_df.repartition(*partition_cols) if partition_cols else clean_df
        w = writer.write.mode("append").format("parquet")
        if partition_cols:
            w = w.partitionBy(*partition_cols)
        w.save(output_path)

    elif write_mode == "upsert":
        upsert_partition(
            batch_df=clean_df,
            silver_path=output_path,
            id_col=config["id_col"],
            timestamp_col=config["timestamp_col"],
            partition_cols=partition_cols,
            target_schema=config["silver_schema"],
        )

    elif write_mode == "overwrite":
        (clean_df.write.mode("overwrite").format("parquet").save(output_path))

    else:
        raise ValueError(
            f"[PIPELINE] Unknown write_mode '{write_mode}' for '{output_path}'"
        )


def _run_dataset(
    spark: SparkSession,
    name: str,
    config: dict,
    input_base_path: str,
    output_base_path: str,
    quarantine_base_path: str,
    batch_logical_date: str,
) -> None:
    """Based on Audit-Write-Audit-Publish pattern, but smaller"""
    after_mandatory = quarantine_mandatory = after_dedup = quarantine_duplicates = None
    try:
        reader_options = config.get("reader_options", {})
        if config["reader_type"] == "json":
            raw_df = read_bronze_json(
                spark, input_base_path, config["input_path"], **reader_options
            )
        else:
            raw_df = read_bronze_csv(spark, input_base_path, config["input_path"])

        after_mandatory, quarantine_mandatory = audit_mandatory_columns(
            raw_df, config["mandatory_cols"], source_name=name
        )

        silver_df = config["transform_func"](after_mandatory)

        if config["write_mode"] == "upsert":
            after_dedup, quarantine_duplicates = audit_duplicates(
                silver_df,
                id_col=config["id_col"],
                timestamp_col=config["timestamp_col"],
                source_name=name,
            )
        else:
            after_dedup = silver_df
            quarantine_duplicates = raw_df.limit(0)

        clean_df = add_audit_columns(after_dedup, batch_logical_date)

        all_quarantine = _stringify_quarantine(quarantine_mandatory).unionByName(
            _stringify_quarantine(quarantine_duplicates), allowMissingColumns=True
        )
        write_quarantine(
            all_quarantine,
            f"{quarantine_base_path.rstrip('/')}/{config['quarantine_path'].lstrip('/')}",
        )

        output_path = (
            f"{output_base_path.rstrip('/')}/{config['output_path'].lstrip('/')}"
        )
        _write_dataset(clean_df, output_path, config)

        logger.info("Successfully processed %s → %s", name, output_path)

    except Exception as e:
        logger.error("Error processing %s: %s", name, e)
        raise
    finally:
        for df in [
            after_dedup,
            quarantine_duplicates,
            after_mandatory,
            quarantine_mandatory,
        ]:
            if df is not None:
                df.unpersist()


def process_bronze_to_silver(
    spark: SparkSession,
    input_base_path: str,
    output_base_path: str,
    quarantine_base_path: str,
    batch_logical_date: str,
) -> None:
    """Orchestrates the bronze-to-silver transformation for all datasets."""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    for name, config in DATASETS.items():
        logger.info("Processing dataset: %s", name)
        _run_dataset(
            spark, name, config, input_base_path, output_base_path,
            quarantine_base_path, batch_logical_date,
        )
