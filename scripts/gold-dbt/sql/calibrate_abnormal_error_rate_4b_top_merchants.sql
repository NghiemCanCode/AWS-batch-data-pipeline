-- STEP 9 measure 4b — head of the flagged list (diagnostic, not one of the
-- four). Shows whether the top is dominated by merchants sitting just above
-- the volume floor. Held at floor 50 on purpose: this is the query that has to
-- answer "is floor 50 good enough", so it must keep showing what floor 50 lets
-- in even after measures 2-4 moved to a list of candidate floors.
--
-- The 2026-07-24 run answered that with a clear no — the top was merchants with
-- 51-66 transactions — but it took manual arithmetic to see why, because the
-- output had only the ratio. failed_count and expected_failed_at_baseline are
-- now printed alongside so the reader sees "3 failures where 0.8 were expected"
-- directly. Two things become obvious from those columns: at ~50 transactions
-- one extra failure moves the rate by ~2 percentage points, so thresholds finer
-- than that are false precision; and a high-volume merchant a little over the
-- threshold is far stronger evidence than a low-volume one far over it, even
-- though sorting by ratio puts the latter on top.
--
-- NO `limit` clause here on purpose: `dbt show --limit N` appends the text
-- `limit N` to the end of this file rather than wrapping it in a subquery, so a
-- limit of our own would compile to `limit 20 limit 25` and fail to parse
-- (hit on the 2026-07-24 run). The row cap belongs on the command line only.
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
-- The portfolio-wide rate of measure 1, recomputed here so this file stays
-- self-contained and the comparison can never drift out of sync with the window.
portfolio as (
    select cast(sum(failed_count) * 1.0 / nullif(sum(txn_count), 0) as double)
        as baseline_rate
    from merchant_window
),
qualified as (
    select
        merchant_id,
        txn_count,
        failed_count,
        cast(failed_count * 100.0 / nullif(txn_count, 0) as double) as error_rate_pct
    from merchant_window
    where txn_count >= 50
)
select
    q.merchant_id,
    q.txn_count,
    q.failed_count,
    -- Alias kept under 20 characters: agate truncates longer headers to
    -- `expected_failed_a...` (README section 16.4).
    cast(q.txn_count * p.baseline_rate as decimal(6, 2)) as expected_failed,
    cast(q.error_rate_pct as decimal(6, 3))              as error_rate_pct
from qualified q
cross join portfolio p
order by q.error_rate_pct desc
