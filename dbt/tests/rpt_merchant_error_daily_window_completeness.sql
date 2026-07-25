-- spec (docs/metrics/merchant_error_daily_report.md section 9): "Window
-- completeness" — the half that a column-level accepted_range cannot express.
--
-- window_day_count must be 30 on every row whose date_key is at least 29 days
-- after the first day present in the source; only the opening 29 days of
-- history are allowed a truncated window (Decision Log 2026-07-25, which chose
-- to emit those rows rather than start the table at history + 29). The
-- `between 1 and 30` half of the same check lives on the column as a
-- dbt_utils.accepted_range in
-- models/marts/reporting/rpt_merchant_error_daily.yml.
--
-- This catches the failure mode the range test cannot see: a window_day_count
-- that is inside 1..30 but wrong — a spine that starts late, a frame narrower
-- than 29 preceding rows, or a history_start that drifted. Those would leave
-- the _30d columns quietly under-counted, which in this table also means an
-- under-counted DENOMINATOR and therefore merchants flagged for nothing, while
-- every range check stays green.
--
-- history_start comes from fact_daily_transaction_trend, not from
-- fact_transactions as in the rpt_card_portfolio sibling: the trend fact is
-- this model's only source, so it is the same bound the model itself computes.
-- The two happen to be equal today (the trend fact is built from
-- fact_transactions and drops no row), but reading the model's actual source
-- keeps this check honest if that ever stops being true.
--
-- Deliberately NOT scoped to the batch day, unlike the reconciliation test.
-- This one reads only the model's own column plus a single scalar, so scanning
-- every partition costs almost nothing, and full-history scope is what makes it
-- able to catch a backfilled partition that was written with the wrong window.
--
-- Any row returned is a violation.

with history_start as (

    select to_date(cast(min(date_key) as string), 'yyyyMMdd') as history_start_date
    from {{ ref('fact_daily_transaction_trend') }}

)

select
    rmed.date_key,
    rmed.merchant_id,
    rmed.window_day_count
from {{ ref('rpt_merchant_error_daily') }} as rmed
-- history_start is a single row, so this cross join cannot fan out.
cross join history_start as hs
where to_date(cast(rmed.date_key as string), 'yyyyMMdd')
      >= date_add(hs.history_start_date, 29)
  and rmed.window_day_count != 30
