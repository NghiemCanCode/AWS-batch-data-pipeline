-- STEP 9 measure 5 — is there ANY merchant-level error signal to detect?
--
-- Not one of the four measures in metrics_layer.md section 2.1. Added after the
-- 2026-07-24 runs, which showed something the four prescribed measures cannot
-- express: at every floor and threshold the flagged count is at or BELOW what
-- random variation around the portfolio baseline would produce on its own.
-- Calibrating a threshold is pointless if that holds, so it has to be tested
-- before any parameter is chosen — otherwise we would be tuning the cutoff of a
-- list made entirely of false positives.
--
-- The test is a chi-square goodness of fit. If merchants only differ by chance,
-- each merchant's failure count is Poisson/binomial around n * baseline_rate,
-- and sum((observed - expected)^2 / expected) is distributed as chi-square with
-- (merchants - 1) degrees of freedom, so the ratio chi_square / df sits near 1.
--
--   dispersion ~ 1.0   no merchant effect — differences are sampling noise, and
--                      any "abnormal" shortlist is false positives. The metric
--                      cannot be calibrated on this data at any threshold.
--   dispersion > ~1.5  merchants genuinely differ; calibrating a threshold is
--                      meaningful, and measures 3 and 4 say where to put it.
--
-- Read it per floor: a real effect concentrated in high-volume merchants would
-- show dispersion rising with the floor.
--
-- Caveat this does NOT settle: dispersion ~ 1 says the data holds no detectable
-- merchant effect, not that no merchant ever has a real problem. A small effect
-- would need far more transactions per merchant to surface than this window has
-- (p95 = 14 transactions per merchant per 30 days).
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
        sum(t.failed_transaction_count) as failed_count
    from {{ ref('fact_daily_transaction_trend') }} t
    join window_days w on t.date_key = w.date_key
    group by t.merchant_id
),
floors as (
    select floor_txn from values (50), (100), (200) as f(floor_txn)
),
baseline as (
    select cast(sum(failed_count) * 1.0 / nullif(sum(txn_count), 0) as double) as rate
    from merchant_window
),
observed as (
    select
        f.floor_txn,
        m.failed_count,
        m.txn_count * b.rate as expected_failed
    from merchant_window m
    join floors f on m.txn_count >= f.floor_txn
    cross join baseline b
)
select
    floor_txn,
    count(*) as merchants,
    cast(sum(pow(failed_count - expected_failed, 2) / nullif(expected_failed, 0))
         as decimal(12, 1)) as chi_square,
    count(*) - 1 as df,
    cast(sum(pow(failed_count - expected_failed, 2) / nullif(expected_failed, 0))
         / nullif(count(*) - 1, 0) as decimal(8, 3)) as dispersion
from observed
group by floor_txn
order by floor_txn
