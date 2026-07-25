-- STEP 9 measure 1 — portfolio baseline error rate, plus the resolved window
-- bounds. Method and how to read the result: docs/metrics/abnormal_error_rate_calibration.md
with window_days as (
    select date_key
    from (select distinct date_key from {{ ref('fact_daily_transaction_trend') }})
    order by date_key desc
    limit 30
),
merchant_window as (
    select
        t.merchant_id,
        sum(t.transaction_count)        as txn_count,
        sum(t.failed_transaction_count) as failed_count,
        sum(t.total_spend_amount) + sum(t.total_inflow_amount) as gross_value
    from {{ ref('fact_daily_transaction_trend') }} t
    join window_days w on t.date_key = w.date_key
    group by t.merchant_id
)
-- Exactly 6 columns, and every alias under 20 characters: `dbt show` renders
-- with agate defaults max_columns=6 / max_column_width=20 and silently drops
-- the rest (README section 16.4). window_day_count was dropped for that budget —
-- window_start and window_end already show the span.
select
    (select min(date_key) from window_days) as window_start,
    (select max(date_key) from window_days) as window_end,
    count(*)          as merchants_total,
    sum(txn_count)    as txn_total,
    sum(failed_count) as failed_total,
    cast(sum(failed_count) * 100.0 / nullif(sum(txn_count), 0) as decimal(6, 3))
        as error_rate_pct
from merchant_window
