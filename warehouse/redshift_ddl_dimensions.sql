-- Date Dimension
CREATE TABLE IF NOT EXISTS date_dimension (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month SMALLINT,
    day_of_year SMALLINT,
    week_of_month SMALLINT,
    week_of_year SMALLINT,
    month SMALLINT,
    month_name VARCHAR(10),
    quarter SMALLINT,
    year SMALLINT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100),
    is_today BOOLEAN,
    is_yesterday BOOLEAN,
    is_last_7_days BOOLEAN,
    is_last_30_days BOOLEAN,
    is_last_90_days BOOLEAN,
    is_last_365_days BOOLEAN,
    PRIMARY KEY (date_key)
)
-- DISTSTYLE ALL because this dimension is used in many fact table.
DISTSTYLE ALL
SORTKEY (date_key);

-- Time Dimension
CREATE TABLE IF NOT EXISTS time_dimension (
    time_key INT NOT NULL,
    time_24h VARCHAR(8),
    hour_24 SMALLINT,
    hour_12 SMALLINT,
    am_pm VARCHAR(2),
    minute SMALLINT,
    second SMALLINT,
    time_bucket_15min VARCHAR(5),
    time_bucket_hourly VARCHAR(5),
    day_part VARCHAR(20),
    business_hours BOOLEAN,
    PRIMARY KEY (time_key)
)
DISTSTYLE ALL
SORTKEY (time_key);

-- Customer Dimension (SCD 2)
CREATE TABLE IF NOT EXISTS customer_dimension (
    -- Primary key
    customer_key INT IDENTITY(1,1) NOT NULL,

    customer_id VARCHAR(20) NOT NULL,
    current_age SMALLINT,
    retirement_age SMALLINT,
    birth_month SMALLINT,
    birth_year SMALLINT,
    gender VARCHAR(10),
    address VARCHAR(30),

    -- SCD Type 2 fields
    per_capital_income DECIMAL(18,2),
    per_cap_income_date_created DATE NOT NULL,
    is_current_per_cap BOOLEAN NOT NULL,
    version_number_per_cap SMALLINT NOT NULL,

    year_income DECIMAL(18,2),
    year_income_date_created DATE NOT NULL,
    is_current_year_income BOOLEAN NOT NULL,
    version_number_year_income SMALLINT NOT NULL,

    PRIMARY KEY (customer_key)
)
DISTSTYLE ALL
SORTKEY (customer_key);

-- Account Dimension (SCD 2)
CREATE TABLE IF NOT EXISTS account_dimension (
    -- Primary Key
    account_key INT IDENTITY(1,1) NOT NULL,

    account_id VARCHAR(20) NOT NULL,
    card_type VARCHAR(10) NOT NULL,
    card_number VARCHAR(16) NOT NULL,
    express_month SMALLINT,
    express_year SMALLINT,
    ccv VARCHAR(3),
    has_chip BOOLEAN,

    -- SCD Type 2 fields
    num_card_issue SMALLINT,
    date_created DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version_number SMALLINT NOT NULL,

    PRIMARY KEY (account_key)
)

DISTSTYLE ALL
SORTKEY (account_key);

-- Merchant Dimension
CREATE TABLE IF NOT EXISTS merchant_dimension (
    merchant_key INT NOT NULL,
    merchant_category_code VARCHAR(4),
    merchant_category VARCHAR(20),

    PRIMARY KEY (merchant_key)
)
DISTSTYLE ALL
SORTKEY (merchant_key);

-- Location Dimension
CREATE TABLE IF NOT EXISTS location_dimension (
    location_key INT NOT NULL,
    city VARCHAR(20),
    state VARCHAR(20),

    PRIMARY KEY (location_key)
)
DISTSTYLE ALL
SORTKEY (location_key);

-- Transaction Type Dimension
CREATE TABLE IF NOT EXISTS transaction_type_dimension (
    trans_type_key INT NOT NULL,
    trans_type VARCHAR(20),

    PRIMARY KEY (trans_type_key)
)
DISTSTYLE ALL
SORTKEY (trans_type_key);

-- Transaction Error Type Dimension
CREATE TABLE IF NOT EXISTS trans_error_type_dimension (
    trans_error_type_key INT NOT NULL,
    trans_error_type VARCHAR(20),

    PRIMARY KEY (trans_error_type_key)
)
DISTSTYLE ALL
SORTKEY (trans_error_type_key);

-- Currency Dimension
CREATE TABLE IF NOT EXISTS currency_dimension (
    currency_key INT NOT NULL,
    currency_type VARCHAR(5),

    PRIMARY KEY (currency_key)
)
DISTSTYLE ALL
SORTKEY (currency_key);