"""
Unit tests for bronze_to_silver.py

Tests the Bronze-to-Silver pipeline's core components:
  - Data quality gates (audit_mandatory_columns, audit_duplicates)
  - Schema enforcement (schema_enforcing)
  - Per-dataset transform logic (transactions, cards, users, mcc)
  - System audit columns (add_audit_columns)
"""

import sys
import os
from decimal import Decimal
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src"))
)

from unittest.mock import patch

from aws_pipeline.transformation.bronze_to_silver import (
    audit_mandatory_columns,
    audit_duplicates,
    transform_transactions,
    transform_cards,
    transform_users,
    transform_mcc,
    _run_dataset,
    DATASETS,
)
from aws_pipeline.utils.audit_helpers import schema_enforcing, add_audit_columns
from aws_pipeline.schemas.silver_schema import (
    TransactionsSilverSchema,
    CardsSilverSchema,
    UsersSilverSchema,
    MccSilverSchema,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _string_schema(columns):
    """All-StringType StructType — mimics raw CSV read where every column is a string."""
    return StructType([StructField(c, StringType(), True) for c in columns])


# ──────────────────────────────────────────────
# audit_mandatory_columns
# ──────────────────────────────────────────────


class TestAuditMandatoryColumns:
    """Contract: rows with NULL in any mandatory column go to quarantine.
    This is the first data quality gate — must never let NULL keys into Silver.
    """

    def test_all_rows_clean_when_no_nulls(self, spark):
        data = [("1", "Alice"), ("2", "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])

        clean, corrupted = audit_mandatory_columns(df, ["id", "name"], "test")

        assert clean.count() == 2
        assert corrupted.count() == 0

    def test_null_mandatory_col_routes_to_quarantine(self, spark):
        data = [("1", "Alice"), (None, "Bob"), ("3", None)]
        df = spark.createDataFrame(data, ["id", "name"])

        clean, corrupted = audit_mandatory_columns(df, ["id", "name"], "test")

        assert clean.count() == 1
        assert corrupted.count() == 2

    def test_quarantine_metadata_columns_present(self, spark):
        data = [(None, "Alice")]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        _, corrupted = audit_mandatory_columns(df, ["id"], "transactions")

        for col in [
            "_quarantine_source",
            "_quarantine_reason",
            "_quarantine_null_cols",
            "_quarantine_ts",
        ]:
            assert col in corrupted.columns, f"Missing quarantine column: {col}"

    def test_quarantine_reason_and_source_correct(self, spark):
        data = [(None, "Alice")]
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        _, corrupted = audit_mandatory_columns(df, ["id"], "transactions")
        row = corrupted.collect()[0]

        assert row["_quarantine_source"] == "transactions"
        assert row["_quarantine_reason"] == "null_mandatory_column"

    def test_clean_rows_preserve_original_data(self, spark):
        data = [("1", "Alice"), (None, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])

        clean, _ = audit_mandatory_columns(df, ["id"], "test")
        row = clean.collect()[0]

        assert row["id"] == "1"
        assert row["name"] == "Alice"


# ──────────────────────────────────────────────
# audit_duplicates
# ──────────────────────────────────────────────


class TestAuditDuplicates:
    """Contract: keeps only the latest record per id (by timestamp desc).
    Protects against source re-sends and ensures SCD correctness for upsert datasets.
    """

    def test_no_duplicates_all_rows_pass(self, spark):
        data = [("1", "2023-01-01"), ("2", "2023-01-02")]
        df = spark.createDataFrame(data, ["id", "ts"])

        clean, quarantine = audit_duplicates(df, "id", "ts", "test")

        assert clean.count() == 2
        assert quarantine.count() == 0

    def test_keeps_latest_by_timestamp(self, spark):
        data = [
            ("1", "2023-01-01"),
            ("1", "2023-06-15"),
            ("1", "2023-03-10"),
        ]
        df = spark.createDataFrame(data, ["id", "ts"])

        clean, quarantine = audit_duplicates(df, "id", "ts", "test")

        assert clean.count() == 1
        assert quarantine.count() == 2
        assert clean.collect()[0]["ts"] == "2023-06-15"

    def test_quarantine_metadata_correct(self, spark):
        data = [("1", "2023-01-01"), ("1", "2023-06-15")]
        df = spark.createDataFrame(data, ["id", "ts"])

        _, quarantine = audit_duplicates(df, "id", "ts", "cards")
        row = quarantine.collect()[0]

        assert row["_quarantine_source"] == "cards"
        assert row["_quarantine_reason"] == "duplicate"

    def test_multiple_ids_deduped_independently(self, spark):
        data = [
            ("1", "2023-01-01"),
            ("1", "2023-06-15"),
            ("2", "2023-02-01"),
            ("2", "2023-09-01"),
        ]
        df = spark.createDataFrame(data, ["id", "ts"])

        clean, quarantine = audit_duplicates(df, "id", "ts", "test")

        assert clean.count() == 2
        assert quarantine.count() == 2


# ──────────────────────────────────────────────
# schema_enforcing
# ──────────────────────────────────────────────


class TestSchemaEnforcing:
    """Contract: output schema matches target exactly — correct types, correct columns,
    correct order. This is the last defense before writing to Silver.
    """

    def test_casts_columns_to_target_types(self, spark):
        data = [("1", "10.5", "100")]
        df = spark.createDataFrame(data, ["id", "amount", "count"])

        target = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("amount", DecimalType(18, 2), True),
                StructField("count", IntegerType(), True),
            ]
        )

        result = schema_enforcing(df, target)

        assert result.schema == target
        row = result.collect()[0]
        assert row["id"] == 1
        assert row["amount"] == Decimal("10.50")
        assert row["count"] == 100

    def test_missing_column_filled_with_null(self, spark):
        data = [("1",)]
        df = spark.createDataFrame(data, ["id"])

        target = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("amount", DecimalType(18, 2), True),
            ]
        )

        result = schema_enforcing(df, target)

        row = result.collect()[0]
        assert row["id"] == 1
        assert row["amount"] is None

    def test_extra_columns_dropped(self, spark):
        data = [("1", "extra_value")]
        df = spark.createDataFrame(data, ["id", "extra_col"])

        target = StructType([StructField("id", IntegerType(), True)])

        result = schema_enforcing(df, target)

        assert result.columns == ["id"]
        assert "extra_col" not in result.columns

    def test_output_column_order_matches_target_schema(self, spark):
        """Column order must match target — wrong order breaks downstream joins and writes."""
        data = [("hello", "1")]
        df = spark.createDataFrame(data, ["name", "id"])

        target = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        result = schema_enforcing(df, target)

        assert result.columns == ["id", "name"]


# ──────────────────────────────────────────────
# transform_transactions
# ──────────────────────────────────────────────

TRANSACTIONS_COLS = [
    "id",
    "date",
    "client_id",
    "card_id",
    "amount",
    "use_chip",
    "merchant_id",
    "merchant_city",
    "merchant_state",
    "zip",
    "mcc",
    "errors",
]


class TestTransformTransactions:
    def _make_df(self, spark, overrides=None):
        base = {
            "id": "1",
            "date": "2023-10-27 10:00:00",
            "client_id": "100",
            "card_id": "200",
            "amount": "$50.00",
            "use_chip": "Chip Transaction",
            "merchant_id": "500",
            "merchant_city": "  City  ",
            "merchant_state": "  ca  ",
            "zip": "12345.0",
            "mcc": "1234",
            "errors": "",
        }
        if overrides:
            base.update(overrides)
        row = tuple(base[c] for c in TRANSACTIONS_COLS)
        return spark.createDataFrame([row], _string_schema(TRANSACTIONS_COLS))

    def test_id_renamed_to_transaction_id(self, spark):
        result = transform_transactions(self._make_df(spark))

        assert "transaction_id" in result.columns
        assert "id" not in result.columns

    def test_partition_columns_derived_from_timestamp(self, spark):
        result = transform_transactions(self._make_df(spark))
        row = result.collect()[0]

        assert row["year"] == 2023
        assert row["month"] == 10
        assert row["day"] == 27

    def test_amount_cleaned_from_currency_format(self, spark):
        df = self._make_df(spark, {"amount": "$1,234.56"})
        row = transform_transactions(df).collect()[0]

        assert row["amount"] == Decimal("1234.56")

    def test_is_error_true_when_errors_present(self, spark):
        df = self._make_df(spark, {"errors": "Bad CVV,Technical Glitch"})
        row = transform_transactions(df).collect()[0]

        assert row["is_error"] is True

    def test_is_error_false_when_no_errors(self, spark):
        df = self._make_df(spark, {"errors": ""})
        row = transform_transactions(df).collect()[0]

        assert row["is_error"] is False

    def test_output_contains_all_silver_schema_fields(self, spark):
        result = transform_transactions(self._make_df(spark))
        result_fields = {f.name for f in result.schema.fields}

        for field in TransactionsSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# transform_cards
# ──────────────────────────────────────────────

CARDS_COLS = [
    "id",
    "client_id",
    "card_brand",
    "card_type",
    "card_number",
    "expires",
    "cvv",
    "use_chip",
    "num_cards_issued",
    "credit_limit",
    "acct_open_date",
    "year_pin_last_changed",
    "card_on_dark_web",
]


class TestTransformCards:
    def _make_df(self, spark, overrides=None):
        base = {
            "id": "1",
            "client_id": "101",
            "card_brand": "Visa",
            "card_type": "Credit",
            "card_number": "1234567890",
            "expires": "12/2025",
            "cvv": "123",
            "use_chip": "TRUE",
            "num_cards_issued": "1",
            "credit_limit": "$5,000.00",
            "acct_open_date": "01/2020",
            "year_pin_last_changed": "2023",
            "card_on_dark_web": "FALSE",
        }
        if overrides:
            base.update(overrides)
        row = tuple(base[c] for c in CARDS_COLS)
        return spark.createDataFrame([row], _string_schema(CARDS_COLS))

    def test_id_renamed_to_card_id(self, spark):
        result = transform_cards(self._make_df(spark))

        assert "card_id" in result.columns
        assert "id" not in result.columns

    def test_credit_limit_cleaned_and_cast(self, spark):
        row = transform_cards(self._make_df(spark)).collect()[0]

        assert row["credit_limit"] == Decimal("5000.00")

    def test_card_number_is_masked(self, spark):
        row = transform_cards(self._make_df(spark)).collect()[0]

        assert row["mask_card_number"] == "******7890"

    def test_schema_types(self, spark):
        dtypes = dict(transform_cards(self._make_df(spark)).dtypes)

        assert dtypes["credit_limit"] == "decimal(18,2)"
        assert dtypes["num_cards_issued"] == "int"
        assert dtypes["year_pin_last_changed"] == "int"

    def test_output_contains_all_silver_schema_fields(self, spark):
        result = transform_cards(self._make_df(spark))
        result_fields = {f.name for f in result.schema.fields}

        for field in CardsSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# transform_users
# ──────────────────────────────────────────────

USERS_COLS = [
    "id",
    "current_age",
    "retirement_age",
    "birth_year",
    "birth_month",
    "gender",
    "address",
    "latitude",
    "longitude",
    "per_capita_income",
    "yearly_income",
    "total_debt",
    "credit_score",
    "num_credit_cards",
]


class TestTransformUsers:
    def _make_df(self, spark, overrides=None):
        base = {
            "id": "1",
            "current_age": "30",
            "retirement_age": "65",
            "birth_year": "1990",
            "birth_month": "5",
            "gender": "Male",
            "address": "123 Main St",
            "latitude": "10.0",
            "longitude": "20.0",
            "per_capita_income": "$50,000.00",
            "yearly_income": "$100,000.00",
            "total_debt": "$0.00",
            "credit_score": "70",
            "num_credit_cards": "2",
        }
        if overrides:
            base.update(overrides)
        row = tuple(base[c] for c in USERS_COLS)
        return spark.createDataFrame([row], _string_schema(USERS_COLS))

    def test_id_renamed_to_user_id(self, spark):
        result = transform_users(self._make_df(spark))

        assert "user_id" in result.columns
        assert "id" not in result.columns

    def test_currency_columns_cleaned(self, spark):
        row = transform_users(self._make_df(spark)).collect()[0]

        assert row["per_capita_income"] == Decimal("50000.00")
        assert row["yearly_income"] == Decimal("100000.00")
        assert row["total_debt"] == Decimal("0.00")

    def test_dropped_columns_not_in_output(self, spark):
        """current_age, latitude, longitude are explicitly dropped — not relevant for Silver."""
        result = transform_users(self._make_df(spark))

        for col in ["current_age", "latitude", "longitude"]:
            assert col not in result.columns, f"Should be dropped: {col}"

    def test_integer_types(self, spark):
        dtypes = dict(transform_users(self._make_df(spark)).dtypes)

        assert dtypes["retirement_age"] == "int"
        assert dtypes["birth_year"] == "int"
        assert dtypes["birth_month"] == "int"
        assert dtypes["credit_score"] == "int"

    def test_output_contains_all_silver_schema_fields(self, spark):
        result = transform_users(self._make_df(spark))
        result_fields = {f.name for f in result.schema.fields}

        for field in UsersSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# transform_mcc
# ──────────────────────────────────────────────


class TestTransformMcc:
    def test_unpivots_columns_to_rows(self, spark):
        """Each JSON key becomes a row: (mcc_code, merchant_name)."""
        data = [("Eating Places", "Service Stations")]
        schema = StructType(
            [
                StructField("5812", StringType(), True),
                StructField("5541", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        rows = transform_mcc(df).collect()
        lookup = {r["mcc_code"]: r["merchant_name"] for r in rows}

        assert len(rows) == 2
        assert lookup["5812"] == "Eating Places"
        assert lookup["5541"] == "Service Stations"

    def test_output_contains_all_silver_schema_fields(self, spark):
        data = [("Eating Places",)]
        schema = StructType([StructField("5812", StringType(), True)])
        df = spark.createDataFrame(data, schema)

        result_fields = {f.name for f in transform_mcc(df).schema.fields}

        for field in MccSilverSchema.fields:
            assert field.name in result_fields, f"Missing: {field.name}"


# ──────────────────────────────────────────────
# add_audit_columns
# ──────────────────────────────────────────────


class TestAddAuditColumns:
    """Contract: adds exactly 6 system audit columns required by Silver schema."""

    EXPECTED_AUDIT_COLS = [
        "_created_at",
        "_source_file",
        "_processing_id",
        "_updated_at",
        "_batch_logical_date",
        "_is_deleted",
    ]

    _BATCH_DATE = "2026-01-01"

    def test_adds_all_six_audit_columns(self, spark):
        df = spark.createDataFrame([("1",)], ["id"])
        result = add_audit_columns(df, self._BATCH_DATE)

        for col in self.EXPECTED_AUDIT_COLS:
            assert col in result.columns, f"Missing audit column: {col}"

    def test_original_columns_preserved(self, spark):
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = spark.createDataFrame([("1", "hello")], schema)
        result = add_audit_columns(df, self._BATCH_DATE)

        assert "id" in result.columns
        assert "name" in result.columns
        assert len(result.columns) == 2 + len(self.EXPECTED_AUDIT_COLS)

    def test_timestamps_are_not_null(self, spark):
        df = spark.createDataFrame([("1",)], ["id"])
        row = add_audit_columns(df, self._BATCH_DATE).collect()[0]

        assert row["_created_at"] is not None
        assert row["_updated_at"] is not None
        assert row["_batch_logical_date"] is not None

    def test_processing_id_is_not_null(self, spark):
        df = spark.createDataFrame([("1",)], ["id"])
        row = add_audit_columns(df, self._BATCH_DATE).collect()[0]

        assert row["_processing_id"] is not None

    def test_is_deleted_defaults_to_false(self, spark):
        df = spark.createDataFrame([("1",)], ["id"])
        row = add_audit_columns(df, self._BATCH_DATE).collect()[0]

        assert row["_is_deleted"] is False


# ──────────────────────────────────────────────
# _run_dataset (mcc)
# ──────────────────────────────────────────────


class TestRunDatasetMcc:
    """
    Contract: _run_dataset must process the mcc dataset without errors.

    Root-cause under test:
      DATASETS["mcc"]["mandatory_cols"] = ["mcc_code"], but audit_mandatory_columns
      runs on raw_df BEFORE transform_mcc (unpivot). Raw mcc JSON has MCC codes as
      column names (e.g. "5812", "5541") — the column "mcc_code" does not exist yet.
      This causes an AnalysisException inside audit_mandatory_columns.

    Fix required: mandatory_cols for mcc must reference columns that exist in the raw
    data, or be set to [] since mcc is a full-overwrite reference table.
    """

    _RAW_SCHEMA = StructType(
        [
            StructField("5812", StringType(), True),
            StructField("5541", StringType(), True),
        ]
    )

    def _raw_mcc_df(self, spark):
        return spark.createDataFrame(
            [("Eating Places", "Service Stations")], self._RAW_SCHEMA
        )

    def test_mcc_mandatory_cols_exist_in_raw_data(self, spark):
        """Every column in mandatory_cols must be present in the raw mcc DataFrame.

        Fails today because 'mcc_code' is only created after transform_mcc unpivots
        the data — it does not exist in the raw JSON input.
        """
        raw_df = self._raw_mcc_df(spark)
        mcc_mandatory_cols = DATASETS["mcc"]["mandatory_cols"]

        missing = [c for c in mcc_mandatory_cols if c not in raw_df.columns]
        assert missing == [], (
            f"mandatory_cols {missing} do not exist in raw mcc data "
            f"(raw columns: {raw_df.columns}). "
            f"'mcc_code' is only created after transform_mcc (unpivot). "
            f"Fix: set mandatory_cols to [] for the mcc dataset."
        )

    def test_run_dataset_mcc_produces_correct_output(self, spark, tmp_path):
        """_run_dataset with mcc config must write mcc_code + merchant_name to Silver."""
        raw_df = self._raw_mcc_df(spark)
        output_base = str(tmp_path / "silver")
        quarantine_base = str(tmp_path / "quarantine")

        with patch(
            "aws_pipeline.transformation.bronze_to_silver.read_bronze_json",
            return_value=raw_df,
        ):
            _run_dataset(
                spark=spark,
                name="mcc",
                config=DATASETS["mcc"],
                input_base_path="s3://fake-bucket/",
                output_base_path=output_base,
                quarantine_base_path=quarantine_base,
                batch_logical_date="2026-01-01",
            )

        result = spark.read.parquet(f"{output_base}/silver/mcc")
        lookup = {r["mcc_code"]: r["merchant_name"] for r in result.collect()}

        assert lookup["5812"] == "Eating Places"
        assert lookup["5541"] == "Service Stations"

    def test_run_dataset_mcc_quarantine_is_empty_for_clean_data(self, spark, tmp_path):
        """Clean mcc data must produce zero quarantine rows."""
        raw_df = self._raw_mcc_df(spark)
        output_base = str(tmp_path / "silver")
        quarantine_base = str(tmp_path / "quarantine")

        with patch(
            "aws_pipeline.transformation.bronze_to_silver.read_bronze_json",
            return_value=raw_df,
        ):
            _run_dataset(
                spark=spark,
                name="mcc",
                config=DATASETS["mcc"],
                input_base_path="s3://fake-bucket/",
                output_base_path=output_base,
                quarantine_base_path=quarantine_base,
                batch_logical_date="2026-01-01",
            )

        quarantine = spark.read.parquet(f"{quarantine_base}/mcc")
        assert quarantine.count() == 0
