{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['month_end_date_key'],
    )
}}

-- Periodic snapshot fact (spec docs/facts/user_monthly_snapshot_fact.md):
-- fact-of-fact, sourced only from gold.fact_transactions, not silver
-- (section 4). Grain = 1 row per (month_end_date_key, customer_key)
-- (section 3), no surrogate key (Decision Log 2026-07-23, section 6.1).
--
-- Load strategy: insert_overwrite by month_end_date_key (section 2, Decision
-- Log 2026-07-23) — full history on first run/full-refresh, then only the
-- month containing batch_logical_date() on incremental runs (section 8.1),
-- since correcting/backfilling older closed months isn't handled yet (Open
-- Question #1).

with source_transactions as (

    select
        customer_key,
        card_key,
        transaction_amount,
        is_error,
        cast(
            date_format(last_day(to_date(cast(date_key as string), 'yyyyMMdd')), 'yyyyMMdd')
            as int
        ) as month_end_date_key
    from {{ ref('fact_transactions') }}

    -- customer_key = '-1' (Unknown) is excluded from the aggregate rather
    -- than kept as a sentinel bucket: a monthly customer-behavior snapshot
    -- has no reporting meaning for an unresolved customer (Decision Log
    -- 2026-07-23, section 6.2/8.1) — unlike fact_transactions, where Unknown
    -- rows are kept to avoid losing raw transactions.
    where customer_key != '-1'

    {% if is_incremental() %}
    and date_trunc('month', to_date(cast(date_key as string), 'yyyyMMdd'))
        = date_trunc('month', {{ batch_logical_date() }})
    {% endif %}

),

aggregated as (

    select
        month_end_date_key,
        customer_key,
        cast(count(*) as int) as transaction_count,
        -- card_key = '-1' (unresolved) excluded from distinct_card_count for
        -- the same reason customer_key = '-1' is excluded above: it isn't a
        -- real card.
        cast(count(distinct case when card_key != '-1' then card_key end) as int)
            as distinct_card_count,
        cast(
            sum(case when transaction_amount > 0 then transaction_amount else 0 end)
            as decimal(18, 2)
        ) as total_income_amount,
        cast(
            sum(case when transaction_amount < 0 then abs(transaction_amount) else 0 end)
            as decimal(18, 2)
        ) as total_outcome_amount,
        cast(sum(case when is_error = false then 1 else 0 end) as int)
            as successful_transaction_count
    from source_transactions
    group by month_end_date_key, customer_key

)

select
    month_end_date_key,
    customer_key,
    transaction_count,
    distinct_card_count,
    total_income_amount,
    total_outcome_amount,
    -- Divided by the full days-in-month, not days elapsed so far — same
    -- convention as legacy for a partial (still-open) month (section 5.1,
    -- Open Question #4).
    cast(
        total_income_amount / day(to_date(cast(month_end_date_key as string), 'yyyyMMdd'))
        as decimal(18, 2)
    ) as average_daily_total_income_amount,
    cast(
        total_outcome_amount / day(to_date(cast(month_end_date_key as string), 'yyyyMMdd'))
        as decimal(18, 2)
    ) as average_daily_total_outcome_amount,
    successful_transaction_count
from aggregated
