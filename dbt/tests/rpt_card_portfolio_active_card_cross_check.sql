-- spec (docs/metrics/card_portfolio_report.md section 9): "Cross-check Active
-- Card <-> activity fact" — THE check that lets the Active Card metric have two
-- materialization homes at all.
--
-- The metric registry (docs/metrics/metrics_layer.md section 1) requires
-- exactly one home per metric. Active Card is the first exception:
-- fact_customer_activity_daily is the primary home (broken down by CUSTOMER
-- segment) and rpt_card_portfolio is the secondary one (broken down by CARD
-- attribute), because neither grain can be derived from the other — a distinct
-- count does not sum across grains (registry rule #2). The exception only holds
-- while the two computations are provably identical, which is what this test
-- asserts, EXACTLY and with no tolerance:
--
--     sum(rpt_card_portfolio.active_card_count_90d) at D
--   = sum(fact_customer_activity_daily.active_card_count_90d) at D + delta
--
-- This is a genuine cross-check, not an expression compared against itself: the
-- activity fact reaches the number by collecting per-customer daily card sets
-- and de-duplicating them over a window, while this model counts one row per
-- card per day off a card spine and sums a conditional flag.
--
-- The delta term makes the equality exact rather than approximate. The two
-- tables differ on purpose in exactly one place: fact_customer_activity_daily
-- drops transactions whose customer_key is the Unknown sentinel (its section
-- 6), rpt_card_portfolio keeps them (section 6 here). A card whose transactions
-- in the window ALL carry customer_key = '-1' is therefore invisible to the
-- activity fact while still being active here. delta counts exactly those
-- cards. A card with a mix of resolved and unresolved transactions is visible
-- to both and contributes to neither side of the difference.
--
-- Expected to be 0 on the current dev data (Open Question #4): no transaction
-- has been observed that resolves a card but not a customer. If delta turns out
-- to be persistently > 0, section 9 suggests promoting it to an audit column
-- rather than leaving it buried in a test.
--
-- The test also catches something the activity fact silently assumes: that a
-- card belongs to exactly one customer. If one card_id appeared under two
-- customers, the activity fact would count it once per customer while this
-- model counts it once, and the equality would break.
--
-- Both seed members are excluded when resolving card identity ('-1' Unknown,
-- '-2' Not Applicable), matching both models: neither is a real card.
--
-- Scoped to only the batch day (the partition insert_overwrite last touched),
-- not full history — cheaper per run, matches both models' output scope, so it
-- reuses the same batch_logical_date() macro they use to pick that partition.
--
-- coalesce(..., 0) on both aggregate sides is deliberate: without it, a missing
-- partition makes sum() return NULL, and `NULL != <n>` is NULL, so the test
-- would silently pass on the one failure mode it most needs to catch.
--
-- Note on full-refresh runs with no --vars: batch_logical_date() then falls
-- back to current_date(), and against historical data every side is 0, so this
-- passes trivially. Pass --vars '{batch_logical_date: <data day>}' to check a
-- day both models actually built.

with batch_day as (

    select
        cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key,
        {{ batch_logical_date() }} as batch_date

),

report_total as (

    select coalesce(sum(active_card_count_90d), 0) as active_card_count
    from {{ ref('rpt_card_portfolio') }}
    where date_key = (select date_key from batch_day)

),

activity_total as (

    select coalesce(sum(active_card_count_90d), 0) as active_card_count
    from {{ ref('fact_customer_activity_daily') }}
    where date_key = (select date_key from batch_day)

),

-- One row per card active in the window, flagged with whether ANY of its
-- transactions in that window resolved to a real customer. Grouping on the
-- natural key card_id, not card_key: an SCD2 version change inside the window
-- gives one card two surrogate keys.
window_cards as (

    -- batch_day is a single row, so the cross join can't fan out.
    select
        dca.card_id,
        max(case when ft.customer_key != '-1' then 1 else 0 end) as has_resolved_customer
    from {{ ref('fact_transactions') }} as ft
    inner join {{ ref('dim_cards') }} as dca
        on ft.card_key = dca.card_key
    cross join batch_day as bd
    where ft.card_key not in ('-1', '-2')
      and to_date(cast(ft.date_key as string), 'yyyyMMdd')
          between date_sub(bd.batch_date, 89) and bd.batch_date
    group by dca.card_id

),

delta as (

    select coalesce(sum(case when has_resolved_customer = 0 then 1 else 0 end), 0)
        as delta_card_count
    from window_cards

)

select
    report_total.active_card_count as report_active_card_count,
    activity_total.active_card_count as activity_active_card_count,
    delta.delta_card_count as sentinel_customer_only_card_count
from report_total
cross join activity_total
cross join delta
where report_total.active_card_count
    != activity_total.active_card_count + delta.delta_card_count
