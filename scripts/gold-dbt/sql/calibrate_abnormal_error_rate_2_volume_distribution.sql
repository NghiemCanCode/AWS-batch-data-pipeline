-- STEP 9 measure 2 — merchant volume distribution, and what each candidate
-- volume floor keeps, one row per floor.
--
-- Rewritten after the 2026-07-24 run for three reasons:
--   - it reported floors 50 and 200 as fixed column pairs; floors now come from
--     the shared `floors` CTE (same list as measures 3 and 4), and 100 is in it
--     because measure 4b showed floor 50 admits merchants with ~50-65
--     transactions whose "error rate" is 3 failures worth of noise.
--   - it reported the share CUT. The acceptance criterion is phrased the other
--     way round — the floor may drop many long-tail merchants as long as it
--     KEEPS most of the transaction value — so reporting `kept` removes a
--     mental inversion at the exact point where the judgement is made.
--   - transaction share was not reported at all, only merchant count and gross
--     value. It is the denominator the metric actually runs on.
-- Gross value = spend + inflow (Q&A 2026-07-24), i.e. money moved either way.
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
-- Population stats are floor-independent; they repeat on every row so the whole
-- picture stays readable as one table.
population as (
    select
        count(*)                            as merchants_total,
        sum(txn_count)                      as txn_total,
        sum(gross_value)                    as gross_total,
        percentile_approx(txn_count, 0.50)  as p50_txn,
        percentile_approx(txn_count, 0.75)  as p75_txn,
        percentile_approx(txn_count, 0.90)  as p90_txn,
        percentile_approx(txn_count, 0.95)  as p95_txn
    from merchant_window
),
kept as (
    select
        f.floor_txn,
        count(m.merchant_id) as merchants_kept,
        sum(m.txn_count)     as txn_kept,
        sum(m.gross_value)   as gross_kept
    from floors f
    left join merchant_window m on m.txn_count >= f.floor_txn
    group by f.floor_txn
)
-- Trimmed to 6 columns: `dbt show` renders with agate defaults max_columns=6 /
-- max_column_width=20 and silently DROPS the rest — the 16:44 run lost exactly
-- the three pct_*_kept columns this query exists for (README section 16.4). The
-- population percentiles were the ones cut, because they do not vary by floor
-- and measure 1 plus the 2026-07-24 result already record them
-- (p50/p75/p90/p95 = 2/4/9/14 transactions per merchant per 30 days).
select
    k.floor_txn,
    p.merchants_total,
    k.merchants_kept,
    cast(k.merchants_kept * 100.0 / nullif(p.merchants_total, 0) as decimal(6, 2))
        as pct_merchants_kept,
    cast(k.txn_kept * 100.0 / nullif(p.txn_total, 0) as decimal(6, 2))
        as pct_txn_kept,
    cast(k.gross_kept * 100.0 / nullif(p.gross_total, 0) as decimal(6, 2))
        as pct_gross_kept
from kept k
cross join population p
order by k.floor_txn
