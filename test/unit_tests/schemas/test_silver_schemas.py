"""
Unit tests for silver_schema.py

Validates the Silver layer schema contract: field names, data types,
nullable constraints, and audit column structure. These tests act as a
schema registry guard — any unintended schema drift will break here
before it silently corrupts downstream Gold transforms.
"""

import sys
import os
from pyspark.sql.types import (
    StructType, StringType, TimestampNTZType, DateType,
    DecimalType, IntegerType, BooleanType, ArrayType,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from aws_pipeline.schemas.silver_schema import (
    TIMEZONE_POLICY,
    SYSTEM_AUDIT_COLUMNS,
    TransactionsSilverSchema,
    CardsSilverSchema,
    UsersSilverSchema,
    MccSilverSchema,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _field_names(schema):
    return [f.name for f in schema.fields]


def _field_map(schema):
    return {f.name: f for f in schema.fields}


def _type_map(schema):
    return {f.name: f.dataType for f in schema.fields}


def _nullable_map(schema):
    return {f.name: f.nullable for f in schema.fields}


def _get_not_null_fields(schema):
    return {f.name for f in schema.fields if not f.nullable}


AUDIT_FIELD_NAMES = [
    "_created_at", "_source_file", "_processing_id",
    "_updated_at", "_batch_logical_date", "_is_deleted",
]


# ════════════════════════════════════════════════════
# DATA GOVERNANCE POLICY
# ════════════════════════════════════════════════════

class TestTimezonePolicy:
    """All timestamps in Silver layer MUST be UTC per data governance policy."""

    def test_timezone_policy_is_utc(self):
        assert TIMEZONE_POLICY == "UTC"


# ════════════════════════════════════════════════════
# SYSTEM_AUDIT_COLUMNS
# ════════════════════════════════════════════════════

class TestSystemAuditColumns:
    """Silver audit columns support CDC/SCD with _created_at, _updated_at,
    _batch_logical_date, and _is_deleted alongside lineage fields."""

    def test_has_six_fields(self):
        assert len(SYSTEM_AUDIT_COLUMNS) == 6

    def test_field_names_and_order(self):
        names = [f.name for f in SYSTEM_AUDIT_COLUMNS]
        assert names == AUDIT_FIELD_NAMES

    def test_field_types(self):
        type_map = {f.name: f.dataType for f in SYSTEM_AUDIT_COLUMNS}
        assert isinstance(type_map["_created_at"], TimestampNTZType)
        assert isinstance(type_map["_source_file"], StringType)
        assert isinstance(type_map["_processing_id"], StringType)
        assert isinstance(type_map["_updated_at"], TimestampNTZType)
        assert isinstance(type_map["_batch_logical_date"], TimestampNTZType)
        assert isinstance(type_map["_is_deleted"], BooleanType)

    def test_nullable_expectations(self):
        """_source_file is optional (not all sources have file lineage); the rest are mandatory."""
        nullable_map = {f.name: f.nullable for f in SYSTEM_AUDIT_COLUMNS}
        assert nullable_map["_created_at"] is False
        assert nullable_map["_processing_id"] is False
        assert nullable_map["_updated_at"] is False
        assert nullable_map["_batch_logical_date"] is False
        assert nullable_map["_source_file"] is True
        assert nullable_map["_is_deleted"] is False

    def test_all_schemas_end_with_audit_columns(self):
        """Every Silver schema must append SYSTEM_AUDIT_COLUMNS at the tail."""
        all_schemas = [
            TransactionsSilverSchema,
            CardsSilverSchema,
            UsersSilverSchema,
            MccSilverSchema,
        ]
        for schema in all_schemas:
            tail_fields = schema.fields[-6:]
            tail_names = [f.name for f in tail_fields]
            assert tail_names == AUDIT_FIELD_NAMES, (
                f"Schema {schema} does not end with SYSTEM_AUDIT_COLUMNS. "
                f"Tail: {tail_names}"
            )


# ════════════════════════════════════════════════════
# TransactionsSilverSchema
# ════════════════════════════════════════════════════

class TestTransactionsSilverSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "transaction_id", "timestamp", "client_id", "card_id", "amount",
        "transaction_channel", "merchant_id", "merchant_city",
        "merchant_state", "zip", "mcc", "is_error", "errors",
    ]

    def test_is_struct_type(self):
        assert isinstance(TransactionsSilverSchema, StructType)

    def test_total_field_count(self):
        # 13 business + 6 audit = 19
        assert len(TransactionsSilverSchema.fields) == 19

    def test_contains_all_business_fields(self):
        field_names = _field_names(TransactionsSilverSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_contains_audit_fields(self):
        field_names = _field_names(TransactionsSilverSchema)
        for name in AUDIT_FIELD_NAMES:
            assert name in field_names, f"Missing audit field: {name}"

    def test_primary_key(self):
        """transaction_id is the natural key, StringType, NOT NULL."""
        fm = _field_map(TransactionsSilverSchema)
        assert isinstance(fm["transaction_id"].dataType, StringType)
        assert fm["transaction_id"].nullable is False

    def test_foreign_keys_not_null(self):
        """client_id, card_id, merchant_id are FK references — must be NOT NULL."""
        nm = _nullable_map(TransactionsSilverSchema)
        assert nm["client_id"] is False
        assert nm["card_id"] is False
        assert nm["merchant_id"] is False

    def test_amount_precision(self):
        """Financial amount requires DecimalType(18,2) — not Float/Double."""
        fm = _field_map(TransactionsSilverSchema)
        assert fm["amount"].dataType == DecimalType(18, 2)

    def test_timestamp_type(self):
        tm = _type_map(TransactionsSilverSchema)
        assert isinstance(tm["timestamp"], TimestampNTZType)

    def test_errors_is_array_of_string(self):
        """errors column stores multiple error codes per transaction."""
        tm = _type_map(TransactionsSilverSchema)
        assert isinstance(tm["errors"], ArrayType)
        assert isinstance(tm["errors"].elementType, StringType)

    def test_is_error_is_boolean(self):
        """is_error is a derived flag (NOT NULL) for fast filtering without exploding errors array."""
        fm = _field_map(TransactionsSilverSchema)
        assert isinstance(fm["is_error"].dataType, BooleanType)
        assert fm["is_error"].nullable is False

    def test_key_data_types(self):
        tm = _type_map(TransactionsSilverSchema)
        assert isinstance(tm["transaction_id"], StringType)
        assert isinstance(tm["timestamp"], TimestampNTZType)
        assert isinstance(tm["client_id"], StringType)
        assert isinstance(tm["card_id"], StringType)
        assert isinstance(tm["amount"], DecimalType)
        assert isinstance(tm["transaction_channel"], StringType)
        assert isinstance(tm["merchant_id"], StringType)
        assert isinstance(tm["merchant_city"], StringType)
        assert isinstance(tm["merchant_state"], StringType)
        assert isinstance(tm["zip"], StringType)
        assert isinstance(tm["mcc"], StringType)
        assert isinstance(tm["is_error"], BooleanType)

    def test_no_duplicate_field_names(self):
        names = _field_names(TransactionsSilverSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# CardsSilverSchema
# ════════════════════════════════════════════════════

class TestCardsSilverSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "card_id", "client_id", "card_brand", "card_type",
        "mask_card_number", "expires", "has_a_cvv", "has_chip",
        "num_cards_issued", "credit_limit", "acct_open_date",
        "year_pin_last_changed", "card_on_dark_web",
    ]

    def test_is_struct_type(self):
        assert isinstance(CardsSilverSchema, StructType)

    def test_total_field_count(self):
        # 13 business + 6 audit = 19
        assert len(CardsSilverSchema.fields) == 19

    def test_contains_all_business_fields(self):
        field_names = _field_names(CardsSilverSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        """card_id is the natural key, StringType, NOT NULL."""
        fm = _field_map(CardsSilverSchema)
        assert isinstance(fm["card_id"].dataType, StringType)
        assert fm["card_id"].nullable is False

    def test_foreign_key_not_null(self):
        """client_id references Users — must be NOT NULL."""
        fm = _field_map(CardsSilverSchema)
        assert fm["client_id"].nullable is False

    def test_credit_limit_precision(self):
        """Financial amount requires DecimalType(18,2)."""
        fm = _field_map(CardsSilverSchema)
        assert fm["credit_limit"].dataType == DecimalType(18, 2)

    def test_date_fields_are_date_type(self):
        """expires and acct_open_date must be DateType, not TimestampNTZType or StringType."""
        tm = _type_map(CardsSilverSchema)
        assert isinstance(tm["expires"], DateType)
        assert isinstance(tm["acct_open_date"], DateType)

    def test_boolean_fields(self):
        tm = _type_map(CardsSilverSchema)
        assert isinstance(tm["has_a_cvv"], BooleanType)
        assert isinstance(tm["has_chip"], BooleanType)
        assert isinstance(tm["card_on_dark_web"], BooleanType)

    def test_pii_masked_card_number(self):
        """mask_card_number is StringType (masked, not raw digits)."""
        tm = _type_map(CardsSilverSchema)
        assert isinstance(tm["mask_card_number"], StringType)

    def test_key_data_types(self):
        tm = _type_map(CardsSilverSchema)
        assert isinstance(tm["card_id"], StringType)
        assert isinstance(tm["client_id"], StringType)
        assert isinstance(tm["card_brand"], StringType)
        assert isinstance(tm["card_type"], StringType)
        assert isinstance(tm["mask_card_number"], StringType)
        assert isinstance(tm["expires"], DateType)
        assert isinstance(tm["has_a_cvv"], BooleanType)
        assert isinstance(tm["has_chip"], BooleanType)
        assert isinstance(tm["num_cards_issued"], IntegerType)
        assert isinstance(tm["credit_limit"], DecimalType)
        assert isinstance(tm["acct_open_date"], DateType)
        assert isinstance(tm["year_pin_last_changed"], IntegerType)
        assert isinstance(tm["card_on_dark_web"], BooleanType)

    def test_no_duplicate_field_names(self):
        names = _field_names(CardsSilverSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# UsersSilverSchema
# ════════════════════════════════════════════════════

class TestUsersSilverSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "user_id", "retirement_age", "birth_year", "birth_month",
        "gender", "address", "per_capita_income", "yearly_income",
        "total_debt", "credit_score", "num_credit_cards",
    ]

    def test_is_struct_type(self):
        assert isinstance(UsersSilverSchema, StructType)

    def test_total_field_count(self):
        # 11 business + 6 audit = 17
        assert len(UsersSilverSchema.fields) == 17

    def test_contains_all_business_fields(self):
        field_names = _field_names(UsersSilverSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        """user_id is the natural key, StringType, NOT NULL."""
        fm = _field_map(UsersSilverSchema)
        assert isinstance(fm["user_id"].dataType, StringType)
        assert fm["user_id"].nullable is False

    def test_monetary_fields_precision(self):
        """All monetary fields must use DecimalType(18,2) for financial accuracy."""
        tm = _type_map(UsersSilverSchema)
        assert tm["per_capita_income"] == DecimalType(18, 2)
        assert tm["yearly_income"] == DecimalType(18, 2)
        assert tm["total_debt"] == DecimalType(18, 2)

    def test_pii_fields_are_nullable(self):
        """PII fields (address, income, credit_score) should be nullable for privacy compliance."""
        nm = _nullable_map(UsersSilverSchema)
        assert nm["address"] is True
        assert nm["per_capita_income"] is True
        assert nm["yearly_income"] is True
        assert nm["credit_score"] is True

    def test_key_data_types(self):
        tm = _type_map(UsersSilverSchema)
        assert isinstance(tm["user_id"], StringType)
        assert isinstance(tm["retirement_age"], IntegerType)
        assert isinstance(tm["birth_year"], IntegerType)
        assert isinstance(tm["birth_month"], IntegerType)
        assert isinstance(tm["gender"], StringType)
        assert isinstance(tm["address"], StringType)
        assert isinstance(tm["per_capita_income"], DecimalType)
        assert isinstance(tm["yearly_income"], DecimalType)
        assert isinstance(tm["total_debt"], DecimalType)
        assert isinstance(tm["credit_score"], IntegerType)
        assert isinstance(tm["num_credit_cards"], IntegerType)

    def test_no_duplicate_field_names(self):
        names = _field_names(UsersSilverSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# MccSilverSchema
# ════════════════════════════════════════════════════

class TestMccSilverSchema:
    EXPECTED_BUSINESS_FIELDS = ["mcc_code", "merchant_name"]

    def test_is_struct_type(self):
        assert isinstance(MccSilverSchema, StructType)

    def test_total_field_count(self):
        # 2 business + 6 audit = 8
        assert len(MccSilverSchema.fields) == 8

    def test_contains_all_business_fields(self):
        field_names = _field_names(MccSilverSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        """mcc_code is the natural key, StringType, NOT NULL."""
        fm = _field_map(MccSilverSchema)
        assert isinstance(fm["mcc_code"].dataType, StringType)
        assert fm["mcc_code"].nullable is False

    def test_key_data_types(self):
        tm = _type_map(MccSilverSchema)
        assert isinstance(tm["mcc_code"], StringType)
        assert isinstance(tm["merchant_name"], StringType)

    def test_no_duplicate_field_names(self):
        names = _field_names(MccSilverSchema)
        assert len(names) == len(set(names))
