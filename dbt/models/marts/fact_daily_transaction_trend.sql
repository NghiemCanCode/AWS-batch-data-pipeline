{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['date_key'],
    )
}}

-- Daily aggregate fact (spec docs/facts/daily_transaction_trend_fact.md):
-- fact-of-fact, sourced only from gold.fact_transactions, not silver
-- (section 4). Grain = 1 row per (date_key, mcc, merchant_id) (section 3),
-- no surrogate key (Decision Log 2026-07-23, section 6.1).
--
-- Load strategy: insert_overwrite by date_key (section 2, 8.1) — full history
-- on first run/full-refresh, then only the day of batch_logical_date() on
-- incremental runs, since correcting/backfilling older partitions isn't
-- handled yet (Open Question #1).
--
-- batch_logical_date() means the DATA day, not the day the batch runs: a T+1
-- run on D+1 must be passed `--vars '{batch_logical_date: <D>}'`, otherwise
-- the macro's current_date() default overwrites an empty D+1 partition and
-- leaves D untouched (Q&A decision 2026-07-24).
--
-- error_rate is deliberately NOT a column here: a ratio isn't additive, so
-- averaging it across a roll-up is wrong. Consumers compute
-- failed_transaction_count / transaction_count from the two sums, which is
-- correct at any aggregation level (section 5.1, Decision Log 2026-07-23).

with source_transactions as (

    select
        date_key,
        mcc,
        merchant_id,
        customer_key,
        card_key,
        transaction_amount,
        is_error
    from {{ ref('fact_transactions') }}

    -- No sentinel rows are filtered out here, unlike fact_user_monthly_snapshot
    -- which drops customer_key = '-1': volume and errors of transactions whose
    -- category or customer didn't resolve are still information for this table,
    -- and keeping them is what makes the reconciliation checks (section 9) tie
    -- back to fact_transactions exactly (Decision Log 2026-07-23, section 6.2).
    -- The '-1' sentinels are excluded only inside the distinct counts below.

    {% if is_incremental() %}
    where date_key = cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int)
    {% endif %}

)

select
    date_key,
    mcc,
    merchant_id,
    cast(count(*) as int) as transaction_count,
    -- Spend/inflow are split by sign rather than stored as a net amount, same
    -- convention as fact_user_monthly_snapshot's outcome/income (section 5.1,
    -- Decision Log 2026-07-23); spend is reported as a positive number.
    cast(
        sum(case when transaction_amount < 0 then abs(transaction_amount) else 0 end)
        as decimal(18, 2)
    ) as total_spend_amount,
    cast(
        sum(case when transaction_amount > 0 then transaction_amount else 0 end)
        as decimal(18, 2)
    ) as total_inflow_amount,
    -- is_error is not_null on fact_transactions, so these two always sum back
    -- to transaction_count (section 9 "Success/failed consistency").
    cast(sum(case when is_error = false then 1 else 0 end) as int)
        as successful_transaction_count,
    cast(sum(case when is_error = true then 1 else 0 end) as int)
        as failed_transaction_count,
    -- Distinct counts are only valid AT THIS GRAIN — they must not be summed
    -- when rolling up to category or across days; that has to be computed from
    -- fact_transactions instead (section 5.1). The '-1' keys are dropped here
    -- because they aren't a real card/customer, matching the convention in
    -- fact_user_monthly_snapshot.distinct_card_count.
    cast(count(distinct case when card_key != '-1' then card_key end) as int)
        as unique_cards,
    cast(count(distinct case when customer_key != '-1' then customer_key end) as int)
        as unique_customers
from source_transactions
group by date_key, mcc, merchant_id
