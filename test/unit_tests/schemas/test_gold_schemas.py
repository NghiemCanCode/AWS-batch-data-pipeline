import sys
import os
from pyspark.sql.types import (
    StructType, StringType, TimestampNTZType,
    DecimalType, IntegerType, DoubleType, DateType,
    BooleanType, ShortType, FloatType
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from aws_pipeline.schemas.gold_schema import (
    TIMEZONE_POLICY,
    SYSTEM_AUDIT_COLUMNS,
    DateDimensionSchema,
    TimeDimensionSchema,
    CustomerDimensionSchema,
    AccountDimensionSchema,
    MerchantDimensionSchema,
    LocationDimensionSchema,
    TransactionTypeDimensionSchema,
    TransErrorTypeDimensionSchema,
    CurrencyDimensionSchema,
    AccountTransactionFactSchema,
    AccountMonthlySnapshotFactSchema,
    TemporalTrendFactSchema,
    AccountOwnerFactlessSchema,
)


# ════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════

def _field_names(schema):
    """Extract field names from a schema."""
    return [f.name for f in schema.fields]


def _field_map(schema):
    """Build a dict of {field_name: StructField} for easy lookup."""
    return {f.name: f for f in schema.fields}


def _type_map(schema):
    """Build a dict of {field_name: dataType} for easy lookup."""
    return {f.name: f.dataType for f in schema.fields}


def _nullable_map(schema):
    """Build a dict of {field_name: nullable} for easy lookup."""
    return {f.name: f.nullable for f in schema.fields}


def _get_not_null_fields(schema):
    """Returns set of field names that are NOT nullable."""
    return {f.name for f in schema.fields if not f.nullable}


def _get_nullable_fields(schema):
    """Returns set of field names that are nullable."""
    return {f.name for f in schema.fields if f.nullable}


AUDIT_FIELD_NAMES = [
    "_created_at", "_source_file", "_processing_id",
    "_updated_at", "_batch_logical_date", "_is_deleted"
]

SCD2_FIELD_NAMES = [
    "effective_from_date", "effective_to_date", "is_current", "version_number"
]


# ════════════════════════════════════════════════════
# DATA GOVERNANCE POLICY
# ════════════════════════════════════════════════════

class TestTimezonePolicy:
    """All timestamps in Gold layer MUST be UTC per data governance policy."""

    def test_timezone_policy_is_utc(self):
        assert TIMEZONE_POLICY == "UTC"


# ════════════════════════════════════════════════════
# SYSTEM_AUDIT_COLUMNS
# ════════════════════════════════════════════════════

class TestSystemAuditColumns:
    """
    Gold layer audit columns enrich silver audit with _updated_at,
    _batch_logical_date, and _is_deleted for CDC/SCD support.
    """

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
        """_source_file & _is_deleted are optional; the rest are mandatory."""
        nullable_map = {f.name: f.nullable for f in SYSTEM_AUDIT_COLUMNS}
        # Mandatory audit fields
        assert nullable_map["_created_at"] is False
        assert nullable_map["_processing_id"] is False
        assert nullable_map["_updated_at"] is False
        assert nullable_map["_batch_logical_date"] is False
        # Optional audit fields
        assert nullable_map["_source_file"] is True
        assert nullable_map["_is_deleted"] is True

    def test_all_schemas_end_with_audit_columns(self):
        """Every Gold schema must append SYSTEM_AUDIT_COLUMNS at the tail."""
        all_schemas = [
            DateDimensionSchema, TimeDimensionSchema,
            CustomerDimensionSchema, AccountDimensionSchema,
            MerchantDimensionSchema, LocationDimensionSchema,
            TransactionTypeDimensionSchema, TransErrorTypeDimensionSchema,
            CurrencyDimensionSchema,
            AccountTransactionFactSchema, AccountMonthlySnapshotFactSchema,
            TemporalTrendFactSchema, AccountOwnerFactlessSchema,
        ]
        for schema in all_schemas:
            tail_fields = schema.fields[-6:]
            tail_names = [f.name for f in tail_fields]
            assert tail_names == AUDIT_FIELD_NAMES, (
                f"Schema {schema} does not end with SYSTEM_AUDIT_COLUMNS. "
                f"Tail: {tail_names}"
            )


# ════════════════════════════════════════════════════
# DateDimensionSchema
# ════════════════════════════════════════════════════

class TestDateDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "date_key", "full_date", "day_of_week", "day_of_month",
        "day_of_year", "week_of_month", "week_of_year",
        "month", "month_name", "quarter", "year",
        "is_weekend", "is_holiday", "holiday_name"
    ]

    def test_is_struct_type(self):
        assert isinstance(DateDimensionSchema, StructType)

    def test_total_field_count(self):
        # 14 business + 6 audit = 20
        assert len(DateDimensionSchema.fields) == 20

    def test_contains_all_business_fields(self):
        field_names = _field_names(DateDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_contains_audit_fields(self):
        field_names = _field_names(DateDimensionSchema)
        for name in AUDIT_FIELD_NAMES:
            assert name in field_names, f"Missing audit field: {name}"

    def test_primary_key(self):
        """date_key is a smart key (IntegerType), NOT NULL."""
        fm = _field_map(DateDimensionSchema)
        assert isinstance(fm["date_key"].dataType, IntegerType)
        assert fm["date_key"].nullable is False

    def test_full_date_is_date_type(self):
        """full_date must be DateType, not TimestampNTZType or StringType."""
        tm = _type_map(DateDimensionSchema)
        assert isinstance(tm["full_date"], DateType)

    def test_key_data_types(self):
        tm = _type_map(DateDimensionSchema)
        assert isinstance(tm["date_key"], IntegerType)
        assert isinstance(tm["full_date"], DateType)
        assert isinstance(tm["day_of_week"], StringType)
        assert isinstance(tm["day_of_month"], ShortType)
        assert isinstance(tm["day_of_year"], ShortType)
        assert isinstance(tm["week_of_month"], ShortType)
        assert isinstance(tm["week_of_year"], ShortType)
        assert isinstance(tm["month"], ShortType)
        assert isinstance(tm["month_name"], StringType)
        assert isinstance(tm["quarter"], ShortType)
        assert isinstance(tm["year"], ShortType)
        assert isinstance(tm["is_weekend"], BooleanType)
        assert isinstance(tm["is_holiday"], BooleanType)
        assert isinstance(tm["holiday_name"], StringType)

    def test_not_null_constraints(self):
        """Only date_key and full_date are NOT NULL in business fields."""
        nn = _get_not_null_fields(DateDimensionSchema)
        assert "date_key" in nn
        assert "full_date" in nn

    def test_no_duplicate_field_names(self):
        names = _field_names(DateDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# TimeDimensionSchema
# ════════════════════════════════════════════════════

class TestTimeDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "time_key", "time_24h", "hour_24", "hour_12",
        "am_pm", "minute", "second",
        "time_bucket_15min", "time_bucket_hourly",
        "day_part", "business_hours"
    ]

    def test_is_struct_type(self):
        assert isinstance(TimeDimensionSchema, StructType)

    def test_total_field_count(self):
        # 11 business + 6 audit = 17
        assert len(TimeDimensionSchema.fields) == 17

    def test_contains_all_business_fields(self):
        field_names = _field_names(TimeDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        """time_key is a smart key (IntegerType), NOT NULL."""
        fm = _field_map(TimeDimensionSchema)
        assert isinstance(fm["time_key"].dataType, IntegerType)
        assert fm["time_key"].nullable is False

    def test_key_data_types(self):
        tm = _type_map(TimeDimensionSchema)
        assert isinstance(tm["time_key"], IntegerType)
        assert isinstance(tm["time_24h"], StringType)
        assert isinstance(tm["hour_24"], ShortType)
        assert isinstance(tm["hour_12"], ShortType)
        assert isinstance(tm["am_pm"], StringType)
        assert isinstance(tm["minute"], ShortType)
        assert isinstance(tm["second"], ShortType)
        assert isinstance(tm["time_bucket_15min"], StringType)
        assert isinstance(tm["time_bucket_hourly"], StringType)
        assert isinstance(tm["day_part"], StringType)
        assert isinstance(tm["business_hours"], BooleanType)

    def test_not_null_constraints(self):
        nn = _get_not_null_fields(TimeDimensionSchema)
        assert "time_key" in nn

    def test_no_duplicate_field_names(self):
        names = _field_names(TimeDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# CustomerDimensionSchema (SCD Type 2)
# ════════════════════════════════════════════════════

class TestCustomerDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "customer_key", "customer_id",
        "birth_month", "birth_year", "gender",
        "income_bracket", "address_key"
    ]

    def test_is_struct_type(self):
        assert isinstance(CustomerDimensionSchema, StructType)

    def test_total_field_count(self):
        # 7 business + 4 SCD + 6 audit = 17
        assert len(CustomerDimensionSchema.fields) == 17

    def test_contains_all_business_fields(self):
        field_names = _field_names(CustomerDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_contains_scd2_fields(self):
        """SCD Type 2 dimension must have SCD tracking fields."""
        field_names = _field_names(CustomerDimensionSchema)
        for name in SCD2_FIELD_NAMES:
            assert name in field_names, f"Missing SCD2 field: {name}"

    def test_surrogate_key(self):
        """customer_key = MD5(customer_id + effective_from_date), StringType, NOT NULL."""
        fm = _field_map(CustomerDimensionSchema)
        assert isinstance(fm["customer_key"].dataType, StringType)
        assert fm["customer_key"].nullable is False

    def test_natural_key(self):
        """customer_id is the natural key from source, NOT NULL."""
        fm = _field_map(CustomerDimensionSchema)
        assert isinstance(fm["customer_id"].dataType, StringType)
        assert fm["customer_id"].nullable is False

    def test_sensitive_pii_fields_are_nullable(self):
        """Quasi-identifiers (birth_month, birth_year, gender) should be nullable for privacy."""
        nm = _nullable_map(CustomerDimensionSchema)
        assert nm["birth_month"] is True
        assert nm["birth_year"] is True
        assert nm["gender"] is True
        assert nm["income_bracket"] is True

    def test_scd2_field_types(self):
        tm = _type_map(CustomerDimensionSchema)
        assert isinstance(tm["effective_from_date"], TimestampNTZType)
        assert isinstance(tm["effective_to_date"], TimestampNTZType)
        assert isinstance(tm["is_current"], BooleanType)
        assert isinstance(tm["version_number"], ShortType)

    def test_scd2_fields_are_not_null(self):
        """SCD tracking fields must be NOT NULL to guarantee data integrity."""
        nm = _nullable_map(CustomerDimensionSchema)
        assert nm["effective_from_date"] is False
        assert nm["effective_to_date"] is False
        assert nm["is_current"] is False
        assert nm["version_number"] is False

    def test_key_data_types(self):
        tm = _type_map(CustomerDimensionSchema)
        assert isinstance(tm["customer_key"], StringType)
        assert isinstance(tm["customer_id"], StringType)
        assert isinstance(tm["birth_month"], ShortType)
        assert isinstance(tm["birth_year"], ShortType)
        assert isinstance(tm["gender"], StringType)
        assert isinstance(tm["income_bracket"], StringType)
        assert isinstance(tm["address_key"], StringType)

    def test_no_duplicate_field_names(self):
        names = _field_names(CustomerDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# AccountDimensionSchema (SCD Type 2)
# ════════════════════════════════════════════════════

class TestAccountDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "account_key", "account_id", "card_type",
        "mask_card_number", "expires_month", "expires_year",
        "has_a_cvv", "has_chip", "num_card_issue"
    ]

    def test_is_struct_type(self):
        assert isinstance(AccountDimensionSchema, StructType)

    def test_total_field_count(self):
        # 9 business + 4 SCD + 6 audit = 19
        assert len(AccountDimensionSchema.fields) == 19

    def test_contains_all_business_fields(self):
        field_names = _field_names(AccountDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_contains_scd2_fields(self):
        """SCD Type 2 dimension must have SCD tracking fields."""
        field_names = _field_names(AccountDimensionSchema)
        for name in SCD2_FIELD_NAMES:
            assert name in field_names, f"Missing SCD2 field: {name}"

    def test_surrogate_key(self):
        """account_key = MD5(account_id + effective_from_date), StringType, NOT NULL."""
        fm = _field_map(AccountDimensionSchema)
        assert isinstance(fm["account_key"].dataType, StringType)
        assert fm["account_key"].nullable is False

    def test_natural_key(self):
        """account_id is the natural key from source, NOT NULL."""
        fm = _field_map(AccountDimensionSchema)
        assert isinstance(fm["account_id"].dataType, StringType)
        assert fm["account_id"].nullable is False

    def test_pii_masked_field(self):
        """mask_card_number must be StringType + NOT NULL (always masked)."""
        fm = _field_map(AccountDimensionSchema)
        assert isinstance(fm["mask_card_number"].dataType, StringType)
        assert fm["mask_card_number"].nullable is False

    def test_scd2_fields_are_not_null(self):
        nm = _nullable_map(AccountDimensionSchema)
        assert nm["effective_from_date"] is False
        assert nm["effective_to_date"] is False
        assert nm["is_current"] is False
        assert nm["version_number"] is False

    def test_ml_feature_fields_are_nullable(self):
        """ML feature fields (has_a_cvv, has_chip, num_card_issue) can be nullable."""
        nm = _nullable_map(AccountDimensionSchema)
        assert nm["has_a_cvv"] is True
        assert nm["has_chip"] is True
        assert nm["num_card_issue"] is True

    def test_key_data_types(self):
        tm = _type_map(AccountDimensionSchema)
        assert isinstance(tm["account_key"], StringType)
        assert isinstance(tm["account_id"], StringType)
        assert isinstance(tm["card_type"], StringType)
        assert isinstance(tm["mask_card_number"], StringType)
        assert isinstance(tm["expires_month"], ShortType)
        assert isinstance(tm["expires_year"], ShortType)
        assert isinstance(tm["has_a_cvv"], BooleanType)
        assert isinstance(tm["has_chip"], BooleanType)
        assert isinstance(tm["num_card_issue"], ShortType)

    def test_no_duplicate_field_names(self):
        names = _field_names(AccountDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# MerchantDimensionSchema (Static Dimension)
# ════════════════════════════════════════════════════

class TestMerchantDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "merchant_key", "merchant_category_code", "merchant_category"
    ]

    def test_is_struct_type(self):
        assert isinstance(MerchantDimensionSchema, StructType)

    def test_total_field_count(self):
        # 3 business + 6 audit = 9
        assert len(MerchantDimensionSchema.fields) == 9

    def test_contains_all_business_fields(self):
        field_names = _field_names(MerchantDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        """Static dimension uses natural key (MCC code) → IntegerType, NOT NULL."""
        fm = _field_map(MerchantDimensionSchema)
        assert isinstance(fm["merchant_key"].dataType, IntegerType)
        assert fm["merchant_key"].nullable is False

    def test_no_scd_fields(self):
        """Static dimension should NOT have SCD2 fields."""
        field_names = _field_names(MerchantDimensionSchema)
        for scd_field in SCD2_FIELD_NAMES:
            assert scd_field not in field_names, (
                f"Static dimension should not have SCD field: {scd_field}"
            )

    def test_key_data_types(self):
        tm = _type_map(MerchantDimensionSchema)
        assert isinstance(tm["merchant_key"], IntegerType)
        assert isinstance(tm["merchant_category_code"], StringType)
        assert isinstance(tm["merchant_category"], StringType)

    def test_no_duplicate_field_names(self):
        names = _field_names(MerchantDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# LocationDimensionSchema
# ════════════════════════════════════════════════════

class TestLocationDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "location_key", "city", "state", "zip"
    ]

    def test_is_struct_type(self):
        assert isinstance(LocationDimensionSchema, StructType)

    def test_total_field_count(self):
        # 4 business + 6 audit = 10
        assert len(LocationDimensionSchema.fields) == 10

    def test_contains_all_business_fields(self):
        field_names = _field_names(LocationDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_surrogate_key(self):
        """location_key = MD5(city + state + zip), StringType, NOT NULL."""
        fm = _field_map(LocationDimensionSchema)
        assert isinstance(fm["location_key"].dataType, StringType)
        assert fm["location_key"].nullable is False

    def test_pii_location_fields_are_nullable(self):
        """PII location fields (city, state, zip) should be nullable."""
        nm = _nullable_map(LocationDimensionSchema)
        assert nm["city"] is True
        assert nm["state"] is True
        assert nm["zip"] is True

    def test_key_data_types(self):
        tm = _type_map(LocationDimensionSchema)
        assert isinstance(tm["location_key"], StringType)
        assert isinstance(tm["city"], StringType)
        assert isinstance(tm["state"], StringType)
        assert isinstance(tm["zip"], StringType)

    def test_no_scd_fields(self):
        """Location is not an SCD dimension in this model."""
        field_names = _field_names(LocationDimensionSchema)
        for scd_field in SCD2_FIELD_NAMES:
            assert scd_field not in field_names

    def test_no_duplicate_field_names(self):
        names = _field_names(LocationDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# TransactionTypeDimensionSchema (Static Dimension)
# ════════════════════════════════════════════════════

class TestTransactionTypeDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = ["trans_type_key", "trans_type"]

    def test_is_struct_type(self):
        assert isinstance(TransactionTypeDimensionSchema, StructType)

    def test_total_field_count(self):
        # 2 business + 6 audit = 8
        assert len(TransactionTypeDimensionSchema.fields) == 8

    def test_contains_all_business_fields(self):
        field_names = _field_names(TransactionTypeDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        """Static dimension, natural key → ShortType, NOT NULL."""
        fm = _field_map(TransactionTypeDimensionSchema)
        assert isinstance(fm["trans_type_key"].dataType, ShortType)
        assert fm["trans_type_key"].nullable is False

    def test_no_scd_fields(self):
        field_names = _field_names(TransactionTypeDimensionSchema)
        for scd_field in SCD2_FIELD_NAMES:
            assert scd_field not in field_names

    def test_key_data_types(self):
        tm = _type_map(TransactionTypeDimensionSchema)
        assert isinstance(tm["trans_type_key"], ShortType)
        assert isinstance(tm["trans_type"], StringType)

    def test_no_duplicate_field_names(self):
        names = _field_names(TransactionTypeDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# TransErrorTypeDimensionSchema (Static Dimension)
# ════════════════════════════════════════════════════

class TestTransErrorTypeDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = ["trans_error_type_key", "trans_error_type"]

    def test_is_struct_type(self):
        assert isinstance(TransErrorTypeDimensionSchema, StructType)

    def test_total_field_count(self):
        # 2 business + 6 audit = 8
        assert len(TransErrorTypeDimensionSchema.fields) == 8

    def test_contains_all_business_fields(self):
        field_names = _field_names(TransErrorTypeDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        fm = _field_map(TransErrorTypeDimensionSchema)
        assert isinstance(fm["trans_error_type_key"].dataType, ShortType)
        assert fm["trans_error_type_key"].nullable is False

    def test_no_scd_fields(self):
        field_names = _field_names(TransErrorTypeDimensionSchema)
        for scd_field in SCD2_FIELD_NAMES:
            assert scd_field not in field_names

    def test_key_data_types(self):
        tm = _type_map(TransErrorTypeDimensionSchema)
        assert isinstance(tm["trans_error_type_key"], ShortType)
        assert isinstance(tm["trans_error_type"], StringType)

    def test_no_duplicate_field_names(self):
        names = _field_names(TransErrorTypeDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# CurrencyDimensionSchema (Static Dimension)
# ════════════════════════════════════════════════════

class TestCurrencyDimensionSchema:
    EXPECTED_BUSINESS_FIELDS = ["currency_key", "currency_type"]

    def test_is_struct_type(self):
        assert isinstance(CurrencyDimensionSchema, StructType)

    def test_total_field_count(self):
        # 2 business + 6 audit = 8
        assert len(CurrencyDimensionSchema.fields) == 8

    def test_contains_all_business_fields(self):
        field_names = _field_names(CurrencyDimensionSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_primary_key(self):
        fm = _field_map(CurrencyDimensionSchema)
        assert isinstance(fm["currency_key"].dataType, ShortType)
        assert fm["currency_key"].nullable is False

    def test_no_scd_fields(self):
        field_names = _field_names(CurrencyDimensionSchema)
        for scd_field in SCD2_FIELD_NAMES:
            assert scd_field not in field_names

    def test_key_data_types(self):
        tm = _type_map(CurrencyDimensionSchema)
        assert isinstance(tm["currency_key"], ShortType)
        assert isinstance(tm["currency_type"], StringType)

    def test_no_duplicate_field_names(self):
        names = _field_names(CurrencyDimensionSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# AccountTransactionFactSchema (Transaction Grain Fact)
# ════════════════════════════════════════════════════

class TestAccountTransactionFactSchema:
    EXPECTED_FK_FIELDS = [
        "date_key", "time_key", "customer_key", "account_key",
        "transaction_type_key", "merchant_key",
        "merchant_location_key", "trans_error_type_key", "currency_key"
    ]

    def test_is_struct_type(self):
        assert isinstance(AccountTransactionFactSchema, StructType)

    def test_total_field_count(self):
        # 1 PK + 9 FK + 1 measure + 6 audit = 17
        assert len(AccountTransactionFactSchema.fields) == 17

    def test_primary_key(self):
        """transaction_id from source, StringType, NOT NULL."""
        fm = _field_map(AccountTransactionFactSchema)
        assert isinstance(fm["transaction_id"].dataType, StringType)
        assert fm["transaction_id"].nullable is False

    def test_contains_all_foreign_keys(self):
        field_names = _field_names(AccountTransactionFactSchema)
        for fk in self.EXPECTED_FK_FIELDS:
            assert fk in field_names, f"Missing FK: {fk}"

    def test_all_foreign_keys_not_null(self):
        """Fact table FK integrity: all foreign keys must be NOT NULL."""
        nm = _nullable_map(AccountTransactionFactSchema)
        for fk in self.EXPECTED_FK_FIELDS:
            assert nm[fk] is False, f"FK {fk} should be NOT NULL"

    def test_foreign_key_types_match_dimension_pks(self):
        """FK types in fact must match PK types in corresponding dimensions."""
        tm = _type_map(AccountTransactionFactSchema)
        # date_key → IntegerType (matches DateDimensionSchema.date_key)
        assert isinstance(tm["date_key"], IntegerType)
        # time_key → IntegerType (matches TimeDimensionSchema.time_key)
        assert isinstance(tm["time_key"], IntegerType)
        # customer_key → StringType (matches CustomerDimensionSchema.customer_key)
        assert isinstance(tm["customer_key"], StringType)
        # account_key → StringType (matches AccountDimensionSchema.account_key)
        assert isinstance(tm["account_key"], StringType)
        # transaction_type_key → ShortType (matches TransactionTypeDimensionSchema.trans_type_key)
        assert isinstance(tm["transaction_type_key"], ShortType)
        # merchant_key → IntegerType (matches MerchantDimensionSchema.merchant_key)
        assert isinstance(tm["merchant_key"], IntegerType)
        # merchant_location_key → StringType (matches LocationDimensionSchema.location_key)
        assert isinstance(tm["merchant_location_key"], StringType)
        # trans_error_type_key → ShortType (matches TransErrorTypeDimensionSchema)
        assert isinstance(tm["trans_error_type_key"], ShortType)
        # currency_key → ShortType (matches CurrencyDimensionSchema.currency_key)
        assert isinstance(tm["currency_key"], ShortType)

    def test_measure_field(self):
        """transaction_amount = DecimalType(18, 2), NOT NULL for financial precision."""
        fm = _field_map(AccountTransactionFactSchema)
        assert fm["transaction_amount"].dataType == DecimalType(18, 2)
        assert fm["transaction_amount"].nullable is False

    def test_measure_precision(self):
        """Financial amount requires DecimalType(18,2) — not Float/Double."""
        tm = _type_map(AccountTransactionFactSchema)
        assert not isinstance(tm["transaction_amount"], (FloatType, DoubleType))

    def test_contains_audit_fields(self):
        field_names = _field_names(AccountTransactionFactSchema)
        for name in AUDIT_FIELD_NAMES:
            assert name in field_names

    def test_no_duplicate_field_names(self):
        names = _field_names(AccountTransactionFactSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# AccountMonthlySnapshotFactSchema (Periodic Snapshot)
# ════════════════════════════════════════════════════

class TestAccountMonthlySnapshotFactSchema:
    EXPECTED_FK_FIELDS = [
        "month_end_date_key", "account_key", "customer_key", "currency_key"
    ]
    EXPECTED_MEASURE_FIELDS = [
        "transaction_count", "total_income_amount", "total_outcome_amount",
        "average_daily_total_income_amount", "average_daily_total_outcome_amount",
        "successful_transaction_count"
    ]

    def test_is_struct_type(self):
        assert isinstance(AccountMonthlySnapshotFactSchema, StructType)

    def test_total_field_count(self):
        # 4 FK + 6 measures + 6 audit = 16
        assert len(AccountMonthlySnapshotFactSchema.fields) == 16

    def test_contains_all_foreign_keys(self):
        field_names = _field_names(AccountMonthlySnapshotFactSchema)
        for fk in self.EXPECTED_FK_FIELDS:
            assert fk in field_names, f"Missing FK: {fk}"

    def test_all_foreign_keys_not_null(self):
        nm = _nullable_map(AccountMonthlySnapshotFactSchema)
        for fk in self.EXPECTED_FK_FIELDS:
            assert nm[fk] is False, f"FK {fk} should be NOT NULL"

    def test_contains_all_measures(self):
        field_names = _field_names(AccountMonthlySnapshotFactSchema)
        for m in self.EXPECTED_MEASURE_FIELDS:
            assert m in field_names, f"Missing measure: {m}"

    def test_all_measures_not_null(self):
        """Snapshot measures must be NOT NULL (use 0 as default, never NULL)."""
        nm = _nullable_map(AccountMonthlySnapshotFactSchema)
        for m in self.EXPECTED_MEASURE_FIELDS:
            assert nm[m] is False, f"Measure {m} should be NOT NULL"

    def test_monetary_measures_are_decimal(self):
        """All monetary fields must use DecimalType(18, 2) for financial accuracy."""
        tm = _type_map(AccountMonthlySnapshotFactSchema)
        monetary_fields = [
            "total_income_amount", "total_outcome_amount",
            "average_daily_total_income_amount",
            "average_daily_total_outcome_amount"
        ]
        for field in monetary_fields:
            assert tm[field] == DecimalType(18, 2), (
                f"{field} should be DecimalType(18,2), got {tm[field]}"
            )

    def test_count_measures_are_integer(self):
        """Count measures should be IntegerType."""
        tm = _type_map(AccountMonthlySnapshotFactSchema)
        assert isinstance(tm["transaction_count"], IntegerType)
        assert isinstance(tm["successful_transaction_count"], IntegerType)

    def test_month_end_date_key_type(self):
        """month_end_date_key is a smart key → IntegerType."""
        tm = _type_map(AccountMonthlySnapshotFactSchema)
        assert isinstance(tm["month_end_date_key"], IntegerType)

    def test_no_duplicate_field_names(self):
        names = _field_names(AccountMonthlySnapshotFactSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# TemporalTrendFactSchema (Aggregated Fact)
# ════════════════════════════════════════════════════

class TestTemporalTrendFactSchema:
    EXPECTED_FK_FIELDS = ["date_key", "time_key", "merchant_key"]
    EXPECTED_MEASURE_FIELDS = [
        "number_transaction", "successful_transaction_count",
        "total_income_amount", "total_outcome_amount"
    ]

    def test_is_struct_type(self):
        assert isinstance(TemporalTrendFactSchema, StructType)

    def test_total_field_count(self):
        # 3 FK + 4 measures + 6 audit = 13
        assert len(TemporalTrendFactSchema.fields) == 13

    def test_contains_all_foreign_keys(self):
        field_names = _field_names(TemporalTrendFactSchema)
        for fk in self.EXPECTED_FK_FIELDS:
            assert fk in field_names, f"Missing FK: {fk}"

    def test_all_foreign_keys_not_null(self):
        nm = _nullable_map(TemporalTrendFactSchema)
        for fk in self.EXPECTED_FK_FIELDS:
            assert nm[fk] is False, f"FK {fk} should be NOT NULL"

    def test_contains_all_measures(self):
        field_names = _field_names(TemporalTrendFactSchema)
        for m in self.EXPECTED_MEASURE_FIELDS:
            assert m in field_names, f"Missing measure: {m}"

    def test_all_measures_not_null(self):
        nm = _nullable_map(TemporalTrendFactSchema)
        for m in self.EXPECTED_MEASURE_FIELDS:
            assert nm[m] is False, f"Measure {m} should be NOT NULL"

    def test_monetary_measures_are_decimal(self):
        tm = _type_map(TemporalTrendFactSchema)
        assert tm["total_income_amount"] == DecimalType(18, 2)
        assert tm["total_outcome_amount"] == DecimalType(18, 2)

    def test_count_measures_are_integer(self):
        tm = _type_map(TemporalTrendFactSchema)
        assert isinstance(tm["number_transaction"], IntegerType)
        assert isinstance(tm["successful_transaction_count"], IntegerType)

    def test_fk_types_match_dimensions(self):
        tm = _type_map(TemporalTrendFactSchema)
        assert isinstance(tm["date_key"], IntegerType)
        assert isinstance(tm["time_key"], IntegerType)
        assert isinstance(tm["merchant_key"], IntegerType)

    def test_no_duplicate_field_names(self):
        names = _field_names(TemporalTrendFactSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# AccountOwnerFactlessSchema (Factless Fact Table)
# ════════════════════════════════════════════════════

class TestAccountOwnerFactlessSchema:
    EXPECTED_BUSINESS_FIELDS = [
        "customer_key", "account_key",
        "valid_from_date", "valid_to_date", "is_active"
    ]

    def test_is_struct_type(self):
        assert isinstance(AccountOwnerFactlessSchema, StructType)

    def test_total_field_count(self):
        # 5 business + 6 audit = 11
        assert len(AccountOwnerFactlessSchema.fields) == 11

    def test_contains_all_business_fields(self):
        field_names = _field_names(AccountOwnerFactlessSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert name in field_names, f"Missing field: {name}"

    def test_all_business_fields_not_null(self):
        """Factless fact: all relationship fields must be NOT NULL."""
        nm = _nullable_map(AccountOwnerFactlessSchema)
        for name in self.EXPECTED_BUSINESS_FIELDS:
            assert nm[name] is False, f"{name} should be NOT NULL"

    def test_fk_types_match_dimensions(self):
        tm = _type_map(AccountOwnerFactlessSchema)
        assert isinstance(tm["customer_key"], StringType)
        assert isinstance(tm["account_key"], StringType)

    def test_temporal_fields(self):
        tm = _type_map(AccountOwnerFactlessSchema)
        assert isinstance(tm["valid_from_date"], TimestampNTZType)
        assert isinstance(tm["valid_to_date"], TimestampNTZType)
        assert isinstance(tm["is_active"], BooleanType)

    def test_no_duplicate_field_names(self):
        names = _field_names(AccountOwnerFactlessSchema)
        assert len(names) == len(set(names))


# ════════════════════════════════════════════════════
# CROSS-SCHEMA: Referential Integrity (FK ↔ PK Type Match)
# ════════════════════════════════════════════════════

class TestCrossSchemaReferentialIntegrity:
    """
    Validate that FK data types in fact schemas exactly match
    the PK data types in their corresponding dimension schemas.
    This prevents silent join failures in Spark at runtime.
    """

    def test_date_key_consistency(self):
        """date_key type must match across DateDimension and all facts using it."""
        pk_type = _type_map(DateDimensionSchema)["date_key"]
        assert _type_map(AccountTransactionFactSchema)["date_key"] == pk_type
        assert _type_map(TemporalTrendFactSchema)["date_key"] == pk_type
        assert _type_map(AccountMonthlySnapshotFactSchema)["month_end_date_key"] == pk_type

    def test_time_key_consistency(self):
        pk_type = _type_map(TimeDimensionSchema)["time_key"]
        assert _type_map(AccountTransactionFactSchema)["time_key"] == pk_type
        assert _type_map(TemporalTrendFactSchema)["time_key"] == pk_type

    def test_customer_key_consistency(self):
        pk_type = _type_map(CustomerDimensionSchema)["customer_key"]
        assert _type_map(AccountTransactionFactSchema)["customer_key"] == pk_type
        assert _type_map(AccountMonthlySnapshotFactSchema)["customer_key"] == pk_type
        assert _type_map(AccountOwnerFactlessSchema)["customer_key"] == pk_type

    def test_account_key_consistency(self):
        pk_type = _type_map(AccountDimensionSchema)["account_key"]
        assert _type_map(AccountTransactionFactSchema)["account_key"] == pk_type
        assert _type_map(AccountMonthlySnapshotFactSchema)["account_key"] == pk_type
        assert _type_map(AccountOwnerFactlessSchema)["account_key"] == pk_type

    def test_merchant_key_consistency(self):
        pk_type = _type_map(MerchantDimensionSchema)["merchant_key"]
        assert _type_map(AccountTransactionFactSchema)["merchant_key"] == pk_type
        assert _type_map(TemporalTrendFactSchema)["merchant_key"] == pk_type

    def test_transaction_type_key_consistency(self):
        pk_type = _type_map(TransactionTypeDimensionSchema)["trans_type_key"]
        assert _type_map(AccountTransactionFactSchema)["transaction_type_key"] == pk_type

    def test_trans_error_type_key_consistency(self):
        pk_type = _type_map(TransErrorTypeDimensionSchema)["trans_error_type_key"]
        assert _type_map(AccountTransactionFactSchema)["trans_error_type_key"] == pk_type

    def test_currency_key_consistency(self):
        pk_type = _type_map(CurrencyDimensionSchema)["currency_key"]
        assert _type_map(AccountTransactionFactSchema)["currency_key"] == pk_type
        assert _type_map(AccountMonthlySnapshotFactSchema)["currency_key"] == pk_type

    def test_location_key_consistency(self):
        pk_type = _type_map(LocationDimensionSchema)["location_key"]
        assert _type_map(AccountTransactionFactSchema)["merchant_location_key"] == pk_type
