
-- FACT_TRANSACTION
CREATE TABLE IF NOT EXISTS fact_transaction (
    fact_transaction_id BIGINT IDENTITY(1,1) NOT NULL,
    transaction_id VARCHAR(100) NOT NULL,

    -- Foreign Keys
    customer_key BIGINT NOT NULL,
    merchant_key BIGINT NOT NULL,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    channel_key SMALLINT NOT NULL,
    location_key INT NOT NULL,

    -- Measures
    transaction_amount DECIMAL(18,2) NOT NULL,
    transaction_count SMALLINT DEFAULT 1,
    is_fraud BOOLEAN DEFAULT FALSE,
    processing_time_seconds DECIMAL(10,2),

    -- Degenerate Dimensions
    transaction_status VARCHAR(20),
    payment_method VARCHAR(50),
    device_type VARCHAR(50),

    -- Audit columns
    created_date TIMESTAMP DEFAULT GETDATE(),
    updated_date TIMESTAMP,
    etl_batch_id VARCHAR(50),

    PRIMARY KEY (fact_transaction_id),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (merchant_key) REFERENCES dim_merchant(merchant_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (date_key, customer_key);

-- FACT_CUSTOMER_DAILY_SUMMARY
CREATE TABLE IF NOT EXISTS fact_customer_daily_summary (
    customer_key BIGINT NOT NULL,
    date_key INT NOT NULL,
    channel_key SMALLINT NOT NULL,

    -- Aggregated Measures
    daily_transaction_count INT,
    daily_transaction_amount DECIMAL(18,2),
    daily_avg_transaction_amount DECIMAL(18,2),
    daily_unique_merchants INT,
    daily_unique_categories INT,
    daily_fraud_count INT,

    -- Audit
    created_date TIMESTAMP DEFAULT GETDATE(),
    updated_date TIMESTAMP,

    PRIMARY KEY (customer_key, date_key, channel_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (date_key, customer_key);

-- FACT_CUSTOMER_MONTHLY_SUMMARY
CREATE TABLE IF NOT EXISTS fact_customer_monthly_summary (
    customer_key BIGINT NOT NULL,
    month_key INT NOT NULL,

    -- Aggregated Measures
    monthly_transaction_count INT,
    monthly_transaction_amount DECIMAL(18,2),
    monthly_avg_transaction_amount DECIMAL(18,2),
    monthly_unique_merchants INT,
    monthly_unique_categories INT,
    monthly_channel_count INT,

    -- Behavioral Metrics
    month_over_month_transaction_change_pct DECIMAL(10,2),
    month_over_month_amount_change_pct DECIMAL(10,2),
    is_churned BOOLEAN,
    is_declining BOOLEAN,

    -- Audit
    created_date TIMESTAMP DEFAULT GETDATE(),
    updated_date TIMESTAMP,

    PRIMARY KEY (customer_key, month_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (month_key) REFERENCES dim_date(date_key)
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (month_key, customer_key);

-- Create additional indexes for performance
CREATE INDEX idx_fact_transaction_date ON fact_transaction(date_key);
CREATE INDEX idx_fact_transaction_merchants ON fact_transaction(merchant_key);
CREATE INDEX idx_fact_transaction_channel ON fact_transaction(channel_key);
CREATE INDEX idx_fact_transaction_id ON fact_transaction(transaction_id);

-- Create indexes for daily summary
CREATE INDEX idx_daily_summary_date ON fact_customer_daily_summary(date_key);

-- Create indexes for monthly summary
CREATE INDEX idx_monthly_summary_month ON fact_customer_monthly_summary(month_key);

