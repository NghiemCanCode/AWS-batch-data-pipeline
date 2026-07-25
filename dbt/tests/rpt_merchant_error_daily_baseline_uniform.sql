-- spec (docs/metrics/merchant_error_daily_report.md section 9): "Baseline
-- uniform per day" — portfolio_error_rate_30d must have exactly ONE distinct
-- value per date_key.
--
-- The baseline is a property of the window, not of the merchant: it is total
-- failures over total transactions across EVERY merchant of that window, taken
-- BEFORE the volume floor is applied (section 8.1 step 6). This check is the
-- structural guard on that ordering. The two mistakes it catches both leave
-- every other test green:
--   * computing the baseline AFTER the floor — it would still be constant per
--     day, so this test alone would not see it; but combined with the spec's
--     expected value (~0.016090 on dev, measured over all 10,433 merchants)
--     a drift is visible on inspection.
--   * computing it per group (per mcc, per merchant segment...) — that DOES
--     produce several values on the same day, which is exactly what this
--     returns.
-- Computing it after the floor would raise the baseline artificially and shrink
-- every excess_failed_transactions_30d systematically, which would silently
-- change the ranking column the whole table exists to provide.
--
-- Deliberately NOT scoped to the batch day, unlike the reconciliation test.
-- This one reads two columns of the model and nothing else, so scanning every
-- partition costs almost nothing, and full-history scope is what makes it able
-- to catch a backfilled partition written by an older/wrong code path.
--
-- Any row returned is a violation.

select
    date_key,
    count(distinct portfolio_error_rate_30d) as distinct_baseline_count
from {{ ref('rpt_merchant_error_daily') }}
group by date_key
having count(distinct portfolio_error_rate_30d) > 1
