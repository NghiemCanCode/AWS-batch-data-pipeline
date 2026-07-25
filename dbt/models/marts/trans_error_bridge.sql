{{
    config(
        materialized='incremental',
        incremental_strategy='append',
    )
}}

with errors_exploded as (

    select
        transaction_id,
        timestamp,
        explode(errors) as error_reason
    from {{ ref('stg_transactions') }}
    where errors is not null

    {% if is_incremental() %}
    and timestamp > (select coalesce(max(timestamp), timestamp'1900-01-01') from {{ this }})
    {% endif %}

)

-- error_reason is kept UPPER-cased, matching silver's normalization
-- (src/aws_pipeline/transformation/data_transform.py clean_trans_errors_col +
-- trim_and_upper_col) and the project-wide convention that category values are
-- stored UPPER. Values outside the 7-value enum — which silver stores as the
-- literal 'UNKNOWN' — are dropped here: spec section 6.3 / Decision Log
-- 2026-07-23 skip out-of-enum values (skip half only — no logging/alerting hook
-- exists in this project yet, same gap as date_key/time_key in
-- fact_transactions.sql).
select distinct
    transaction_id,
    timestamp,
    error_reason
from errors_exploded
where error_reason in (
    'INSUFFICIENT BALANCE', 'BAD CVV', 'BAD EXPIRATION', 'TECHNICAL GLITCH',
    'BAD CARD NUMBER', 'BAD PIN', 'BAD ZIPCODE'
)
