-- DIM_DATE
CREATE TABLE IF NOT EXISTS dim_date (
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
DISTSTYLE ALL
SORTKEY (date_key);

-- DIM_TIME
CREATE TABLE IF NOT EXISTS dim_time (
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
    peak_hours BOOLEAN,
    PRIMARY KEY (time_key)
)
DISTSTYLE ALL
SORTKEY (time_key);

-- DIM_CUSTOMER (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT IDENTITY(1,1) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    customer_segment VARCHAR(50),
    customer_tier VARCHAR(20),
    signup_date DATE,
    customer_age_group VARCHAR(20),
    acquisition_channel VARCHAR(50),

    -- Pre-aggregated metrics (updated periodically)
    lifetime_transaction_count INT,
    lifetime_transaction_value DECIMAL(18,2),
    average_transaction_value DECIMAL(18,2),
    last_transaction_date DATE,
    days_since_last_transaction INT,
    preferred_channel VARCHAR(50),
    preferred_merchant_category VARCHAR(50),

    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    version_number INT NOT NULL,

    PRIMARY KEY (customer_key)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id, effective_date);

-- DIM_MERCHANT
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key BIGINT IDENTITY(1,1) NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    merchant_name VARCHAR(200),
    merchant_category VARCHAR(100),
    merchant_category_group VARCHAR(50),
    merchant_region VARCHAR(50),
    merchant_city VARCHAR(100),
    merchant_country VARCHAR(50),
    is_online_enabled BOOLEAN,
    merchant_tier VARCHAR(20),
    PRIMARY KEY (merchant_key)
)
DISTSTYLE ALL
SORTKEY (merchant_id);

-- DIM_CHANNEL
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key INT IDENTITY(1,1) NOT NULL,
    channel_id VARCHAR(20) NOT NULL,
    channel_type VARCHAR(50),
    channel_category VARCHAR(20),
    channel_description VARCHAR(200),
    is_self_service BOOLEAN,
    requires_internet BOOLEAN,
    PRIMARY KEY (channel_key)
)
DISTSTYLE ALL
SORTKEY (channel_id);

-- DIM_LOCATION
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INT IDENTITY(1,1) NOT NULL,
    location_id VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    state_province VARCHAR(100),
    country VARCHAR(50),
    region VARCHAR(50),
    postal_code VARCHAR(20),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    timezone VARCHAR(50),
    urban_rural VARCHAR(20),
    PRIMARY KEY (location_key)
)
DISTSTYLE ALL
SORTKEY (location_id);

-- DIM_CUSTOMER_SEGMENT (Reference)
CREATE TABLE IF NOT EXISTS dim_customer_segment (
    segment_key INT IDENTITY(1,1) NOT NULL,
    segment_code VARCHAR(50) NOT NULL,
    segment_name VARCHAR(100),
    segment_description VARCHAR(500),
    min_transaction_count INT,
    min_transaction_amount DECIMAL(18,2),
    priority_level SMALLINT,
    PRIMARY KEY (segment_key)
)
DISTSTYLE ALL
SORTKEY (segment_code)