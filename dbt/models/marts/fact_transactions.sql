{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='transaction_id',
        partition_by=['date_key'],
    )
}}

-- Load strategy: incremental merge on transaction_id, filtered by the timestamp
-- watermark below (docs/facts/transactions_fact.md section 8.1, Decision Log
-- 2026-07-23) — silver_transactions has no _updated_at column, and merge keeps
-- the PK uniqueness test (section 3) valid across reruns.
--
-- customer_key/card_key resolve via AS-OF RANGE JOIN on the dimension validity
-- interval [effective_from_date, effective_to_date) — each transaction attaches
-- to the dimension version valid at its timestamp (point-in-time, spec v.0.0.3
-- Decision Log 2026-07-23; replaces the former is_current = true rule, which
-- gave past transactions today's attributes). Run order requirement: snapshot ->
-- dimension -> fact within the same batch (section 8.1 step 3).

with transactions as (

    select
        transaction_id,
        timestamp,
        client_id,
        card_id,
        amount as transaction_amount,
        transaction_type,
        merchant_id,
        merchant_city,
        merchant_state,
        zip,
        mcc,
        is_error,
        cast(date_format(timestamp, 'yyyyMMdd') as int) as date_key,
        cast(date_format(timestamp, 'HHmmss') as int) as time_key
    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where timestamp > (select coalesce(max(timestamp), timestamp'1900-01-01') from {{ this }})
    {% endif %}

),

resolved as (

    select
        transactions.transaction_id,
        transactions.timestamp,
        transactions.date_key,
        transactions.time_key,
        coalesce(dc.customer_key, '-1') as customer_key,
        coalesce(dca.card_key, '-1') as card_key,
        transactions.transaction_type,
        transactions.merchant_id,
        coalesce(dg.location_key, '-1') as merchant_geo_key,
        coalesce(dm.mcc, '-1') as mcc,
        transactions.transaction_amount,
        transactions.is_error,
        -- Age at transaction time, from the same as-of dim_customers version
        -- joined below (spec section 5.1/8.1 step 3b, v.0.0.4). NULL when the
        -- customer doesn't resolve or birth_year is missing — the seeded
        -- Unknown/Not Applicable members carry NULL birth_year, so unresolved
        -- rows (customer_key '-1') fall out to NULL naturally.
        cast(
            case
                when dc.birth_year is not null
                    then year(transactions.timestamp) - dc.birth_year
            end as smallint
        ) as customer_age_at_transaction,
        dd.date_key as matched_date_key,
        dt.time_key as matched_time_key
    from transactions
    left join {{ ref('dim_dates') }} as dd
        on transactions.date_key = dd.date_key
    left join {{ ref('dim_times') }} as dt
        on transactions.time_key = dt.time_key
    -- As-of range joins: [from, to) — a transaction exactly at a version
    -- boundary attaches to the NEWER version. Dimension-side interval
    -- no-overlap/coverage tests (dbt/tests/) guarantee each business key +
    -- timestamp matches at most one version, so these joins cannot fan out.
    left join {{ ref('dim_customers') }} as dc
        on transactions.client_id = dc.customer_id
       and transactions.timestamp >= dc.effective_from_date
       and transactions.timestamp < dc.effective_to_date
    left join {{ ref('dim_cards') }} as dca
        on transactions.card_id = dca.card_id
       and transactions.timestamp >= dca.effective_from_date
       and transactions.timestamp < dca.effective_to_date
    left join {{ ref('dim_geo') }} as dg
        on transactions.merchant_city = dg.city
       and transactions.merchant_state = dg.state
       and transactions.zip = dg.zip
    left join {{ ref('dim_merchant') }} as dm
        on transactions.mcc = dm.mcc

)

-- date_key/time_key: spec Null Handling = "Skip record + log warning", Not Null = Y
-- (section 5.1/7). Filtering rows whose date_key or time_key doesn't resolve against
-- dim_dates/dim_times implements the skip half; there's no logging/alerting hook in
-- this dbt project yet (same gap noted in stg_geo.sql/stg_mcc.sql), so the warning
-- half is not implemented.
select
    transaction_id,
    timestamp,
    date_key,
    time_key,
    customer_key,
    card_key,
    transaction_type,
    merchant_id,
    merchant_geo_key,
    mcc,
    transaction_amount,
    is_error,
    customer_age_at_transaction
from resolved
where matched_date_key is not null
  and matched_time_key is not null
