-- STEP 6d: partition fingerprint, printed before / between / after the two
-- incremental runs. All three printouts must be identical.
--
-- The _30d columns are summed ACROSS MERCHANTS within the one partition, which
-- is the only direction they are additive — never across date_key, where two
-- consecutive windows overlap by 29 days (spec section 5.1, registry rule #7).
-- That makes this a fingerprint, not a metric: window_txns here is NOT "how
-- many transactions happened", it is 30 days of history counted once per
-- qualified merchant.
--
-- No ratio is summed. min/max of portfolio_error_rate_30d are printed instead
-- of a sum precisely because the two must be EQUAL — that is the singular test
-- rpt_merchant_error_daily_baseline_uniform.sql restated in a form you can read
-- with your eyes, and the value itself is the number to compare against the
-- 1.609% baseline that calibration measured (docs/metrics/
-- abnormal_error_rate_calibration.md section 5).
select
    count(*)                                                                        as total_rows,
    sum(case when date_key = 20191031 then 1 else 0 end)                            as partition_rows,
    sum(case when date_key = 20191031 and is_abnormal_error_rate then 1 else 0 end) as flagged_merchants,
    sum(case when date_key = 20191031 then transaction_count_30d else 0 end)        as window_txns,
    sum(case when date_key = 20191031 then failed_transaction_count_30d else 0 end) as window_failed,
    min(case when date_key = 20191031 then portfolio_error_rate_30d end)            as baseline_min,
    max(case when date_key = 20191031 then portfolio_error_rate_30d end)            as baseline_max
from {{ ref('rpt_merchant_error_daily') }}
