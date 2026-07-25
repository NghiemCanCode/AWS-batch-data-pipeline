-- spec (docs/metrics/card_portfolio_report.md section 9): "Window completeness"
-- — the half that a column-level accepted_range cannot express.
--
-- window_day_count must be 90 on every row whose date_key is at least 89 days
-- after the first transaction day; only the opening 89 days of history are
-- allowed a truncated window. The `between 1 and 90` half of the same check
-- lives on the column as a dbt_utils.accepted_range in
-- models/marts/reporting/rpt_card_portfolio.yml.
--
-- This catches the failure mode the range test cannot see: a window_day_count
-- that is inside 1..90 but wrong — a spine that starts late, a frame narrower
-- than 89 preceding rows, or a history_start that drifted. Those would leave
-- the _90d columns quietly under-counted while every range check stays green.
--
-- Deliberately NOT scoped to the batch day, unlike the three reconciliation
-- tests. This one reads only the model's own columns plus a single scalar, so
-- scanning every partition costs almost nothing (tens of thousands of rows for
-- the whole history), and full-history scope is what makes it able to catch a
-- backfilled partition that was written with the wrong window.
--
-- Any row returned is a violation.

with history_start as (

    select to_date(cast(min(date_key) as string), 'yyyyMMdd') as history_start_date
    from {{ ref('fact_transactions') }}

)

select
    rcp.date_key,
    rcp.chip_segment,
    rcp.card_brand,
    rcp.window_day_count
from {{ ref('rpt_card_portfolio') }} as rcp
-- history_start is a single row, so this cross join cannot fan out.
cross join history_start as hs
where to_date(cast(rcp.date_key as string), 'yyyyMMdd')
      >= date_add(hs.history_start_date, 89)
  and rcp.window_day_count != 90
