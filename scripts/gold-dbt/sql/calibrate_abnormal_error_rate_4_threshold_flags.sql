-- STEP 9 measure 4 — how many merchants each candidate threshold would flag,
-- one row per candidate volume floor. The acceptance criterion is operational
-- rather than statistical: the shortlist has to be short enough that Marketing
-- can work through it by hand (~10-50).
--
-- Both grids were refined after the 2026-07-24 run:
--   - thresholds were 3/5/8/10% and came back 28/6/0/0 at floor 50, so 8% and
--     10% catch nobody and 5% (the placeholder) catches only 6 — everything
--     worth deciding sits between 3% and 5%. 3 and 5 are kept so the two runs
--     stay comparable.
--   - the floor was a single hardcoded 50, which measure 4b showed to be too
--     low. Floors now come from the `floors` CTE, shared with measure 3.
-- Read the two dimensions together: a threshold that yields a decent-sized list
-- at floor 50 may leave nothing at floor 200, and that is the real trade-off.
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
-- Six columns only: `dbt show` drops anything past the sixth (README section
-- 16.4) — the 16:44 run lost flagged_gt_5_0pct. The 4.5% column was dropped
-- instead of 5.0% because 5.0% is the placeholder parameter under review and
-- has to stay visible; the 16:44 numbers for 4.5% are in the calibration doc.
select
    floor_txn,
    count(*)                                               as merchants_qualified,
    sum(case when error_rate_pct > 3.0 then 1 else 0 end)  as flagged_gt_3_0pct,
    sum(case when error_rate_pct > 3.5 then 1 else 0 end)  as flagged_gt_3_5pct,
    sum(case when error_rate_pct > 4.0 then 1 else 0 end)  as flagged_gt_4_0pct,
    sum(case when error_rate_pct > 5.0 then 1 else 0 end)  as flagged_gt_5_0pct
from qualified
group by floor_txn
order by floor_txn
