-- STEP 9 measure 3 — error rate distribution within the group that clears the
-- volume floor, one row per candidate floor.
--
-- Rewritten after the 2026-07-24 run. It used to test the single floor 50; the
-- run showed floor 50 is too low (measure 4b came back topped by merchants with
-- 51-66 transactions, i.e. 3-5 failures, which is noise at this baseline). A
-- single floor cannot show that trade-off, so the floors now come from the
-- `floors` CTE and every measure reports one row each — change the list there
-- and measures 3 and 4 follow together.
-- Method: docs/metrics/abnormal_error_rate_calibration.md
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
),
floors as (
    select floor_txn from values (50), (100), (200) as f(floor_txn)
),
qualified as (
    select
        f.floor_txn,
        m.merchant_id,
        m.txn_count,
        cast(m.failed_count * 100.0 / nullif(m.txn_count, 0) as double) as error_rate_pct
    from merchant_window m
    join floors f on m.txn_count >= f.floor_txn
)
select
    floor_txn,
    count(*) as merchants_qualified,
    cast(percentile_approx(error_rate_pct, 0.50) as decimal(6, 3)) as p50_error_pct,
    cast(percentile_approx(error_rate_pct, 0.90) as decimal(6, 3)) as p90_error_pct,
    cast(percentile_approx(error_rate_pct, 0.95) as decimal(6, 3)) as p95_error_pct,
    cast(percentile_approx(error_rate_pct, 0.99) as decimal(6, 3)) as p99_error_pct
from qualified
group by floor_txn
order by floor_txn
