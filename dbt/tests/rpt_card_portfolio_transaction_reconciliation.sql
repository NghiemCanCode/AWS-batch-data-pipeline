-- spec (docs/metrics/card_portfolio_report.md section 9): "Reconcile
-- transactions -> fact_transactions" — the totals of transaction_count_90d,
-- successful_transaction_count_90d and failed_transaction_count_90d across
-- every segment on the batch day must equal the corresponding counts of ALL
-- transactions in that day's trailing 90-day window, counted straight from
-- fact_transactions.
--
-- An EXACT tie-out is possible here precisely because this model drops no rows:
-- transactions on an unresolved card (card_key = '-1') are kept and land in the
-- (UNKNOWN, UNKNOWN) bucket rather than being filtered out (section 6, Decision
-- Log 2026-07-25) — the deliberate opposite of fact_customer_activity_daily,
-- whose totals cannot tie back. No sentinel filter appears on either side of
-- this test for the same reason.
--
-- Section 9 is explicit that this compares against fact_transactions and NOT
-- against fact_daily_transaction_trend: the trend fact groups by mcc, so a
-- fault in that grouping would cancel out on both sides. Here the source is
-- also the model's own build input, so what this check really catches is rows
-- LOST while assigning a segment — an as-of join that silently drops a card
-- instead of pushing it into the UNKNOWN bucket.
--
-- The window is `between date_sub(D, 89) and D`, i.e. (D - 89, D] — 90 calendar
-- days with the current day included, matching the model's
-- `rows between 89 preceding and current row`.
--
-- Scoped to only the batch day (the partition insert_overwrite last touched),
-- not full history — cheaper per run, matches this model's own output scope, so
-- it reuses the same batch_logical_date() macro the model uses to pick that
-- partition.
--
-- coalesce(..., 0) on BOTH sides is deliberate: on the report side a missing
-- partition makes sum() return NULL and `NULL != <n>` is NULL, so the test
-- would silently pass on the one failure mode it most needs to catch; on the
-- source side sum(case ...) is NULL rather than 0 when the window is empty,
-- which would do the same in reverse.
--
-- Note on full-refresh runs with no --vars: batch_logical_date() then falls
-- back to current_date(), and against historical data both sides are 0, so this
-- passes trivially. Pass --vars '{batch_logical_date: <data day>}' to check a
-- day the model actually built.

with batch_day as (

    select
        cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key,
        {{ batch_logical_date() }} as batch_date

),

report_total as (

    select
        coalesce(sum(transaction_count_90d), 0) as transaction_count,
        coalesce(sum(successful_transaction_count_90d), 0) as successful_transaction_count,
        coalesce(sum(failed_transaction_count_90d), 0) as failed_transaction_count
    from {{ ref('rpt_card_portfolio') }}
    where date_key = (select date_key from batch_day)

),

source_total as (

    -- batch_day is a single row, so the cross join can't fan out; it just makes
    -- both ends of the range available without repeating a scalar subquery.
    select
        count(*) as transaction_count,
        coalesce(sum(case when ft.is_error = false then 1 else 0 end), 0)
            as successful_transaction_count,
        coalesce(sum(case when ft.is_error = true then 1 else 0 end), 0)
            as failed_transaction_count
    from {{ ref('fact_transactions') }} as ft
    cross join batch_day as bd
    where to_date(cast(ft.date_key as string), 'yyyyMMdd')
        between date_sub(bd.batch_date, 89) and bd.batch_date

)

select
    report_total.transaction_count as report_transaction_count,
    source_total.transaction_count as source_transaction_count,
    report_total.successful_transaction_count as report_successful_transaction_count,
    source_total.successful_transaction_count as source_successful_transaction_count,
    report_total.failed_transaction_count as report_failed_transaction_count,
    source_total.failed_transaction_count as source_failed_transaction_count
from report_total
cross join source_total
where report_total.transaction_count != source_total.transaction_count
   or report_total.successful_transaction_count != source_total.successful_transaction_count
   or report_total.failed_transaction_count != source_total.failed_transaction_count
