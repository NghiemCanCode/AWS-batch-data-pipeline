-- spec (docs/facts/daily_transaction_trend_fact.md section 9): "Amount
-- reconciliation" — sum(total_spend_amount) - sum(total_inflow_amount) for the
-- day must equal -sum(transaction_amount) of fact_transactions for that same
-- day. That identity holds because spend is stored as a positive number:
--   spend - inflow = -sum(amount < 0) - sum(amount > 0) = -sum(all amounts).
-- Comparing the signed difference (rather than the sum of both columns) is
-- what makes the check sensitive to a row landing on the wrong side of the
-- sign split, which sum(abs(...)) would hide.
--
-- Same batch-day scope, same batch_logical_date() macro and same coalesce
-- rationale as the sibling count reconciliation test.

with batch_day as (

    select cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key

),

trend_total as (

    select
        cast(
            coalesce(sum(total_spend_amount), 0) - coalesce(sum(total_inflow_amount), 0)
            as decimal(18, 2)
        ) as net_outflow_amount
    from {{ ref('fact_daily_transaction_trend') }}
    where date_key = (select date_key from batch_day)

),

source_total as (

    select cast(-coalesce(sum(transaction_amount), 0) as decimal(18, 2)) as net_outflow_amount
    from {{ ref('fact_transactions') }}
    where date_key = (select date_key from batch_day)

)

select
    trend_total.net_outflow_amount as trend_net_outflow_amount,
    source_total.net_outflow_amount as source_net_outflow_amount
from trend_total
cross join source_total
where trend_total.net_outflow_amount != source_total.net_outflow_amount
