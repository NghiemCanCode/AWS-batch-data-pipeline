-- Transaction Fact table
CREATE TABLE IF NOT EXISTS account_transaction_fact (
    -- Primary key
    transaction_id BIGINT NOT NULL UNIQUE,
    -- Foreign key
    date_key INT NOT NULL,
    time_key SMALLINT NOT NULL,
    customer_key INT NOT NULL,
    account_key INT NOT NULL,
    transaction_type_key SMALLINT NOT NULL,
    merchant_key INT NOT NULL,
    merchant_location_key INT NOT NULL,
    trans_error_type_key SMALLINT NOT NULL,
    currency_key SMALLINT NOT NULL,
    -- Measures
    transaction_amount REAL NOT NULL,
    zip VARCHAR(10),
    -- Audit
    created_date TIMESTAMP DEFAULT GETDATE(),
    -- Reference Foreign Key
    PRIMARY KEY (transaction_id),

    FOREIGN KEY (date_key) REFERENCES date_dimension(date_key),
    FOREIGN KEY (time_key) REFERENCES time_dimension(time_key),
    FOREIGN KEY (customer_key) REFERENCES customer_dimension(customer_key),
    FOREIGN KEY (account_key) REFERENCES account_dimension(account_key),
    FOREIGN KEY (transaction_type_key) REFERENCES transaction_type_dimension(trans_type_key),
    FOREIGN KEY (merchant_key) REFERENCES merchant_dimension(merchant_key),
    FOREIGN KEY (merchant_location_key) REFERENCES location_dimension(location_key),
    FOREIGN KEY (trans_error_type_key) REFERENCES trans_error_type_dimension(trans_error_type_key),
    FOREIGN KEY (currency_key) REFERENCES currency_dimension(currency_key)
)
DISTSTYLE ALL
SORTKEY (transaction_id);
-- Monthly Card Snapshot Fact Table
CREATE TABLE IF NOT EXISTS account_monthly_snapshot_fact (
    -- Primary key
    account_monthly_key INT NOT NULL UNIQUE,
    -- Foreign key
    month_end_date_key INT NOT NULL,
    account_key INT NOT NULL,
    customer_key INT NOT NULL,
    -- Metrics
    number_transaction SMALLINT NOT NULL,
    total_income_amount INT NOT NULL,
    total_outcome_amount INT NOT NULL,
    average_daily_total_income_amount FLOAT NOT NULL,
    average_daily_total_outcome_amount FLOAT NOT NULL,
    succession_transaction_count SMALLINT NOT NULL,
    -- Reference Foreign Key
    PRIMARY KEY (account_monthly_key),
    FOREIGN KEY (month_end_date_key) REFERENCES date_dimension(date_key),
    FOREIGN KEY (account_key) REFERENCES account_dimension(account_key),
    FOREIGN KEY (customer_key) REFERENCES customer_dimension(customer_key)

)
DISTSTYLE ALL
SORTKEY (month_end_date_key);

-- Temporal Trend & Seasonal Analysis Process (with Merchant)
CREATE TABLE IF NOT EXISTS temporal_trend_fact (
    -- Primary key
    trend_key INT NOT NULL UNIQUE,
    -- Foreign key
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    merchant_key INT NOT NULL,
    -- Metrics
    number_transaction INT NOT NULL,
    succession_transaction_count REAL NOT NULL,
    total_income_amount REAL NOT NULL,
    total_outcome_amount REAL NOT NULL,
    -- Reference Foreign Key
    PRIMARY KEY (trend_key),

    FOREIGN KEY (date_key) REFERENCES date_dimension(date_key),
    FOREIGN KEY (time_key) REFERENCES time_dimension(time_key),
    FOREIGN KEY (merchant_key) REFERENCES merchant_dimension(merchant_key)
)
DISTSTYLE ALL
SORTKEY (merchant_key);

-- Account Owner Factless Table
CREATE TABLE IF NOT EXISTS account_owner_factless (
    customer_key INT NOT NULL,
    account_key INT NOT NULL,
    FOREIGN KEY (account_key) REFERENCES account_dimension(account_key),
    FOREIGN KEY (customer_key) REFERENCES customer_dimension(customer_key),

    PRIMARY KEY (customer_key, account_key)
)
DISTSTYLE ALL
SORTKEY (customer_key);