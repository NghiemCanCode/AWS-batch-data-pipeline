-- spec (docs/metrics/merchant_error_daily_report.md section 9): "Reconcile ->
-- fact_transactions" — THE MOST IMPORTANT CHECK OF THIS TABLE.
--
-- On the batch day, the set of qualified merchants and their
-- (transaction_count_30d, failed_transaction_count_30d) pair must match a
-- recomputation made INDEPENDENTLY from gold.fact_transactions over the window
-- (D - 29, D]. Matched ABSOLUTELY and in BOTH directions: an extra merchant and
-- a missing merchant both fail.
--
-- Section 9 and the Decision Log are explicit that this recomputes from
-- fact_transactions and NOT from fact_daily_transaction_trend, even though the
-- trend fact is the model's build input. What this check exists to catch is a
-- merchant aggregation that misses another mcc row of the same merchant, or
-- that drops the mcc = '-1' bucket (section 6, registry rule #6) — both
-- understate the DENOMINATOR, inflate the error rate and flag an innocent
-- merchant. A check recomputing from the trend fact would share that fault with
-- the model and pass anyway. Going around the source is the only way to make it
-- independent.
--
-- An EXACT tie-out is possible because the trend fact filters out no source row
-- at all — unlike fact_user_monthly_snapshot and fact_customer_activity_daily,
-- which both drop customer_key = '-1'. For the same reason NO sentinel filter
-- appears on either side of this test.
--
-- The floor is applied on the source side too (the `having` below), because the
-- floor is part of the table's SCOPE, not a filter over an otherwise complete
-- table (section 3). Both sides must therefore agree on which merchants qualify,
-- which is what makes "extra/missing merchant" a real failure mode worth
-- catching rather than an expected difference.
--
-- The window is `between date_sub(D, 29) and D`, i.e. (D - 29, D] — 30 calendar
-- days with the current day included, matching the model's
-- `rows between 29 preceding and current row`.
--
-- Scoped to only the batch day (the partition insert_overwrite last touched),
-- not full history — cheaper per run, matches this model's own output scope, so
-- it reuses the same batch_logical_date() macro the model uses to pick that
-- partition.
--
-- No coalesce(..., 0) trick is needed here, unlike the scalar-total
-- reconciliation tests of rpt_card_portfolio: this compares row sets, so a
-- missing partition surfaces as every source merchant being unmatched rather
-- than as a NULL that quietly swallows the comparison. The two `is null`
-- predicates below are what carry that.
--
-- Note on full-refresh runs with no --vars: batch_logical_date() then falls back
-- to current_date(), and against historical data BOTH sides are empty, so this
-- passes trivially. Pass --vars '{batch_logical_date: <data day>}' to check a
-- day the model actually built.

with batch_day as (

    select
        cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key,
        {{ batch_logical_date() }} as batch_date

),

report_merchants as (

    select
        merchant_id,
        transaction_count_30d,
        failed_transaction_count_30d
    from {{ ref('rpt_merchant_error_daily') }}
    where date_key = (select date_key from batch_day)

),

source_merchants as (

    -- batch_day is a single row, so the cross join can't fan out; it just makes
    -- both ends of the range available without repeating a scalar subquery.
    select
        ft.merchant_id,
        count(*) as transaction_count_30d,
        sum(case when ft.is_error = true then 1 else 0 end) as failed_transaction_count_30d
    from {{ ref('fact_transactions') }} as ft
    cross join batch_day as bd
    where to_date(cast(ft.date_key as string), 'yyyyMMdd')
        between date_sub(bd.batch_date, 29) and bd.batch_date
    group by ft.merchant_id
    having count(*) >= {{ var('abnormal_error_min_transaction_count') }}

)

select
    coalesce(r.merchant_id, s.merchant_id) as merchant_id,
    r.transaction_count_30d as report_transaction_count_30d,
    s.transaction_count_30d as source_transaction_count_30d,
    r.failed_transaction_count_30d as report_failed_transaction_count_30d,
    s.failed_transaction_count_30d as source_failed_transaction_count_30d
from report_merchants as r
full outer join source_merchants as s
    on r.merchant_id = s.merchant_id
-- Any row returned is a violation: merchant only in the report, merchant only
-- in the source, or a merchant present on both sides with different counts.
where r.merchant_id is null
   or s.merchant_id is null
   or r.transaction_count_30d != s.transaction_count_30d
   or r.failed_transaction_count_30d != s.failed_transaction_count_30d
