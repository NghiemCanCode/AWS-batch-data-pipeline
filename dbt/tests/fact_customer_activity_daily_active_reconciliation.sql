-- spec (docs/facts/customer_activity_daily_fact.md section 9): "Spot
-- reconciliation" — the number of customers flagged active on the batch day
-- must equal the number of distinct customers with at least one transaction in
-- that day's trailing 90-day window, counted straight from fact_transactions.
-- This is the check that keeps the model's window logic honest: it re-derives
-- the business spec section 4 definition by a completely different route (a
-- plain date-range filter instead of the model's spine + sliding window), so a
-- frame off by a day or a broken spine shows up here.
--
-- Distinct is taken over customer_id, NOT customer_key: a customer whose SCD2
-- version changes inside the window carries two customer_keys but is still one
-- customer — the same reason the model's grain is the natural key (Decision Log
-- 2026-07-23). customer_key = '-1' is excluded on the source side to match the
-- model's own filter (Decision Log 2026-07-24), otherwise unresolved
-- transactions would inflate this side only.
--
-- The window is `between date_sub(D, 89) and D`, i.e. (D - 89, D] — 90 calendar
-- days with the current day included, matching the model's
-- `rows between 89 preceding and current row`.
--
-- The two sides tie out EXACTLY, not approximately: any customer with a
-- transaction in the window necessarily has their first transaction on or
-- before D, so the spine guarantees them a row on D, and that row's
-- transaction_count_90d is > 0 by construction.
--
-- Scoped to only the batch day (the partition insert_overwrite last touched),
-- not full history — cheaper per run, matches this model's own output scope, so
-- it reuses the same batch_logical_date() macro the model uses to pick that
-- partition.
--
-- coalesce(..., 0) on the activity side is deliberate: without it, a missing
-- partition makes sum() return NULL, and `NULL != <n>` is NULL, so the test
-- would silently pass on the one failure mode it most needs to catch (the same
-- fix the fact_daily_transaction_trend reconciliations carry, and that
-- fact_user_monthly_snapshot's still has as a TODO).
--
-- Note on full-refresh runs with no --vars: batch_logical_date() then falls
-- back to current_date(). Against historical data both sides are 0 and this
-- passes trivially. But if the source data reaches into the last 90 days, the
-- source side counts customers while the activity side is 0 — a full-refresh
-- spine stops at the last day that HAS data, so no partition exists for today.
-- That is the documented behaviour of full-refresh mode (spec section 3), not a
-- bug in this test; pass --vars '{batch_logical_date: <data day>}' to check a
-- day the model actually built.

with batch_day as (

    select
        cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key,
        {{ batch_logical_date() }} as batch_date

),

activity_total as (

    select coalesce(sum(case when is_active_90d then 1 else 0 end), 0)
        as active_customer_count
    from {{ ref('fact_customer_activity_daily') }}
    where date_key = (select date_key from batch_day)

),

source_total as (

    -- batch_day is a single row, so the cross join can't fan out; it just makes
    -- both ends of the range available without repeating a scalar subquery.
    select count(distinct dc.customer_id) as active_customer_count
    from {{ ref('fact_transactions') }} as ft
    inner join {{ ref('dim_customers') }} as dc
        on ft.customer_key = dc.customer_key
    cross join batch_day as bd
    where ft.customer_key != '-1'
      and to_date(cast(ft.date_key as string), 'yyyyMMdd')
          between date_sub(bd.batch_date, 89) and bd.batch_date

)

select
    activity_total.active_customer_count as activity_active_customer_count,
    source_total.active_customer_count as source_active_customer_count
from activity_total
cross join source_total
where activity_total.active_customer_count != source_total.active_customer_count
