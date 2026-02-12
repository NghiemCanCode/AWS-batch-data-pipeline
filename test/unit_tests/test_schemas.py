import pytest
import sys
import os

# Ensure we can import from src if package not installed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from aws_pipeline.schemas.silver_schema import (
    TransactionsSilverSchema,
    CardsSilverSchema,
    UsersSilverSchema,
    MccSilverSchema,
    AUDIT_COLUMNS
)

def test_audit_columns_structure():
    """Verify audit columns definition"""
    field_names = [f.name for f in AUDIT_COLUMNS]
    assert "_ingested_at" in field_names
    assert "_source_file" in field_names
    assert "_processing_id" in field_names

def test_transactions_schema():
    """Verify TransactionsSilverSchema"""
    field_names = [f.name for f in TransactionsSilverSchema.fields]
    assert "id" in field_names
    assert "amount" in field_names
    # Check audit cols are included
    assert "_ingested_at" in field_names

def test_cards_schema():
    """Verify CardsSilverSchema"""
    field_names = [f.name for f in CardsSilverSchema.fields]
    assert "card_number" in field_names
    assert "credit_limit" in field_names
    # Check audit cols are included
    assert "_ingested_at" in field_names

def test_users_schema():
    """Verify UsersSilverSchema"""
    field_names = [f.name for f in UsersSilverSchema.fields]
    assert "current_age" in field_names
    assert "yearly_income" in field_names
    # Check audit cols are included
    assert "_ingested_at" in field_names

def test_mcc_schema():
    """Verify MccSilverSchema"""
    field_names = [f.name for f in MccSilverSchema.fields]
    assert "mcc_code" in field_names
    assert "merchant_name" in field_names
    # Check audit cols are included
    assert "_ingested_at" in field_names
