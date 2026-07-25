{{ config(severity = 'warn') }}

-- spec (docs/metrics/merchant_error_daily_report.md section 9): "Flagged-count
-- in operating band" — the number of merchants flagged on the batch day should
-- sit between roughly 10 and 50 (the operating criterion of
-- abnormal_error_rate_calibration.md section 3; the calibrated parameters
-- produce ~12 on dev).
--
-- Unlike every other check on this table, a violation here is NOT a data error.
-- It says the PARAMETERS have drifted out of usefulness: below the band the
-- metric has gone silent, above it the shortlist is too long to act on — the
-- two failure modes calibration section 1 says are both worse than having no
-- metric. The response is to re-run the calibration, not to fix the model.
--
-- Section 9 rates it Warn, not Critical, and the project has no alert channel
-- yet (transactions_fact.md Open Question #9), so it runs at severity warn:
-- dbt reports it without failing the build. Same treatment as the trend fact's
-- "Unknown MCC share".
--
-- The 10-50 band is hardcoded rather than exposed as dbt vars, unlike the
-- threshold and the floor: it is not a parameter of the METRIC, it is the
-- operating criterion those parameters were chosen to satisfy. Moving it would
-- be moving the goalposts, and has to go through the calibration doc.
--
-- AN EMPTY PARTITION IS SKIPPED (`row_count > 0`), a deliberate deviation from
-- the literal wording of section 9. Without --vars, batch_logical_date() falls
-- back to current_date(), which against historical data selects nothing — so a
-- strict reading would emit a warning on every single dev run and train the
-- reader to ignore this one. Catching "the model wrote zero rows" (the
-- 2036-01-01 sentinel trap of section 2 among others) is the reconciliation
-- test's job, and it does it by comparing row sets rather than by looking at a
-- count. Same reasoning, and the same wording, as the trend fact's
-- unknown-MCC-share test.
--
-- Any row returned is a warning.

with batch_day as (

    select cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key

),

partition_counts as (

    select
        count(*) as row_count,
        sum(case when is_abnormal_error_rate = true then 1 else 0 end) as flagged_count
    from {{ ref('rpt_merchant_error_daily') }}
    where date_key = (select date_key from batch_day)

)

select
    row_count as qualified_merchant_count,
    flagged_count as flagged_merchant_count
from partition_counts
where row_count > 0
  and (flagged_count < 10 or flagged_count > 50)
