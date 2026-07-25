-- STEP 6c diagnostic: the three terms of the Active Card cross-check, printed
-- instead of asserted. The gate is the singular test
-- dbt/tests/rpt_card_portfolio_active_card_cross_check.sql; this exists only to
-- read off the delta term when the test fails, or to confirm it is 0.
--
-- delta_cards = cards whose transactions in the window ALL carry
-- customer_key = '-1'. Those cards are invisible to fact_customer_activity_daily
-- (it drops sentinel-customer transactions) but active in rpt_card_portfolio
-- (it keeps them), so the equality between the two tables only closes after
-- adding this term. Spec docs/metrics/card_portfolio_report.md Open Question #4
-- expects 0 on dev.
--
-- residual must be 0. Anything else means the two homes of the Active Card
-- metric have drifted apart.
--
-- Requires --vars '{batch_logical_date: <data day>}'; with the macro's
-- current_date() default every term is 0 and the output says nothing.
with batch_day as (

    select
        cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key,
        {{ batch_logical_date() }} as batch_date

),

report_total as (

    select coalesce(sum(active_card_count_90d), 0) as active_cards
    from {{ ref('rpt_card_portfolio') }}
    where date_key = (select date_key from batch_day)

),

activity_total as (

    select coalesce(sum(active_card_count_90d), 0) as active_cards
    from {{ ref('fact_customer_activity_daily') }}
    where date_key = (select date_key from batch_day)

),

window_cards as (

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
        as delta_cards
    from window_cards

)

select
    (select date_key from batch_day) as date_key,
    report_total.active_cards as report_active,
    activity_total.active_cards as activity_active,
    delta.delta_cards as delta_cards,
    report_total.active_cards - activity_total.active_cards - delta.delta_cards
        as residual
from report_total
cross join activity_total
cross join delta
