-- spec (docs/facts/daily_transaction_trend_fact.md section 9): "Count
-- reconciliation" — sum(transaction_count) for the day must equal count(*) of
-- fact_transactions for that same day. No sentinel filter on either side,
-- unlike the fact_user_monthly_snapshot equivalents: this model keeps every
-- row of the source (including mcc/customer/card '-1'), which is exactly what
-- makes this check tie out one-to-one (Decision Log 2026-07-23, section 6.2).
--
-- Scoped to only the batch day (the partition insert_overwrite last touched),
-- not full history — cheaper per run, matches this model's own recompute
-- scope, so it reuses the same batch_logical_date() macro the model uses to
-- pick that partition.
--
-- coalesce(..., 0) on the aggregate side is deliberate: without it, a missing
-- partition makes sum() return NULL, and `NULL != <n>` is NULL, so the test
-- would silently pass on the one failure mode it most needs to catch.

with batch_day as (

    select cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key

),

trend_total as (

    select coalesce(sum(transaction_count), 0) as transaction_count
    from {{ ref('fact_daily_transaction_trend') }}
    where date_key = (select date_key from batch_day)

),

source_total as (

    select count(*) as transaction_count
    from {{ ref('fact_transactions') }}
    where date_key = (select date_key from batch_day)

)

select
    trend_total.transaction_count as trend_transaction_count,
    source_total.transaction_count as source_transaction_count
from trend_total
cross join source_total
where trend_total.transaction_count != source_total.transaction_count
