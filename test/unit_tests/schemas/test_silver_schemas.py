import pytest
import sys
import os
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DecimalType, IntegerType, DoubleType
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from aws_pipeline.schemas.silver_schema import (
    TransactionsSilverSchema,
    CardsSilverSchema,
    UsersSilverSchema,
    MccSilverSchema,
    AUDIT_COLUMNS
)


# ──────────────────────────────────────────────
# AUDIT_COLUMNS
# ──────────────────────────────────────────────

class TestAuditColumns:
    def test_has_three_fields(self):
        assert len(AUDIT_COLUMNS) == 3

    def test_field_names(self):
        names = [f.name for f in AUDIT_COLUMNS]
        assert names == ["_ingested_at", "_source_file", "_processing_id"]

    def test_field_types(self):
        type_map = {f.name: f.dataType for f in AUDIT_COLUMNS}
        assert isinstance(type_map["_ingested_at"], TimestampType)
        assert isinstance(type_map["_source_file"], StringType)
        assert isinstance(type_map["_processing_id"], StringType)


# ──────────────────────────────────────────────
# TransactionsSilverSchema
# ──────────────────────────────────────────────

class TestTransactionsSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "id", "date", "client_id", "card_id", "amount", "use_chip",
        "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors"
    ]

    def test_total_field_count(self):
        # 12 business + 3 audit = 15
        assert len(TransactionsSilverSchema.fields) == 15

    def test_contains_all_business_fields(self):
        field_names = [f.name for f in TransactionsSilverSchema.fields]
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_contains_audit_fields(self):
        field_names = [f.name for f in TransactionsSilverSchema.fields]
        for audit_field in AUDIT_COLUMNS:
            assert audit_field.name in field_names

    def test_key_data_types(self):
        type_map = {f.name: f.dataType for f in TransactionsSilverSchema.fields}
        assert isinstance(type_map["date"], TimestampType)
        assert isinstance(type_map["amount"], DecimalType)
        assert type_map["amount"] == DecimalType(10, 2)
        assert isinstance(type_map["id"], StringType)


# ──────────────────────────────────────────────
# CardsSilverSchema
# ──────────────────────────────────────────────

class TestCardsSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "id", "client_id", "card_brand", "card_type", "card_number",
        "expires", "cvv", "has_chip", "num_cards_issued", "credit_limit",
        "acct_open_date", "year_pin_last_changed", "card_on_dark_web"
    ]

    def test_total_field_count(self):
        # 13 business + 3 audit = 16
        assert len(CardsSilverSchema.fields) == 16

    def test_contains_all_business_fields(self):
        field_names = [f.name for f in CardsSilverSchema.fields]
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_key_data_types(self):
        type_map = {f.name: f.dataType for f in CardsSilverSchema.fields}
        assert type_map["credit_limit"] == DecimalType(10, 2)
        assert isinstance(type_map["num_cards_issued"], IntegerType)
        assert isinstance(type_map["year_pin_last_changed"], IntegerType)
        assert isinstance(type_map["card_number"], StringType)


# ──────────────────────────────────────────────
# UsersSilverSchema
# ──────────────────────────────────────────────

class TestUsersSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "id", "current_age", "retirement_age", "birth_year", "birth_month",
        "gender", "address", "latitude", "longitude", "per_capita_income",
        "yearly_income", "total_debt", "credit_score", "num_credit_cards"
    ]

    def test_total_field_count(self):
        # 14 business + 3 audit = 17
        assert len(UsersSilverSchema.fields) == 17

    def test_contains_all_business_fields(self):
        field_names = [f.name for f in UsersSilverSchema.fields]
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_key_data_types(self):
        type_map = {f.name: f.dataType for f in UsersSilverSchema.fields}
        assert type_map["per_capita_income"] == DecimalType(10, 2)
        assert type_map["yearly_income"] == DecimalType(10, 2)
        assert type_map["total_debt"] == DecimalType(10, 2)
        assert isinstance(type_map["current_age"], IntegerType)
        assert isinstance(type_map["latitude"], DoubleType)
        assert isinstance(type_map["longitude"], DoubleType)
        assert isinstance(type_map["credit_score"], IntegerType)


# ──────────────────────────────────────────────
# MccSilverSchema
# ──────────────────────────────────────────────

class TestMccSchema:
    EXPECTED_BUSINESS_FIELDS = ["mcc_code", "merchant_name"]

    def test_total_field_count(self):
        # 2 business + 3 audit = 5
        assert len(MccSilverSchema.fields) == 5

    def test_contains_all_business_fields(self):
        field_names = [f.name for f in MccSilverSchema.fields]
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_key_data_types(self):
        type_map = {f.name: f.dataType for f in MccSilverSchema.fields}
        assert isinstance(type_map["mcc_code"], StringType)
        assert isinstance(type_map["merchant_name"], StringType)
