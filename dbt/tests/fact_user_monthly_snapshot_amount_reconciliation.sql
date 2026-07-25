-- spec (docs/facts/user_monthly_snapshot_fact.md section 9): "Amount
-- reconciliation" — sum(total_income_amount) + sum(total_outcome_amount)
-- for the month must equal sum(abs(transaction_amount)) of
-- fact_transactions for that same month (customer_key = '-1' excluded,
-- matching this model's own filter). Scoped to only the current batch
-- month (not full history), same rationale and same batch_logical_date()
-- macro as the sibling transaction-count reconciliation test.
--
-- TODO: silent pass on a missing partition — if the snapshot has no rows for
-- the batch month, sum(...) is NULL and `NULL != <n>` is NULL, so the test
-- passes instead of flagging the empty month. Wrap the aggregates in
-- coalesce(..., 0) like the fact_daily_transaction_trend reconciliation tests.

with snapshot_month as (

    select
        cast(date_format(last_day({{ batch_logical_date() }}), 'yyyyMMdd') as int)
            as month_end_date_key

),

snapshot_total as (

    select
        cast(sum(total_income_amount) + sum(total_outcome_amount) as decimal(18, 2))
            as total_amount
    from {{ ref('fact_user_monthly_snapshot') }}
    where month_end_date_key = (select month_end_date_key from snapshot_month)

),

source_total as (

    select cast(sum(abs(transaction_amount)) as decimal(18, 2)) as total_amount
    from {{ ref('fact_transactions') }}
    where customer_key != '-1'
      and date_trunc('month', to_date(cast(date_key as string), 'yyyyMMdd'))
          = date_trunc('month', {{ batch_logical_date() }})

)

select
    snapshot_total.total_amount as snapshot_total_amount,
    source_total.total_amount as source_total_amount
from snapshot_total
cross join source_total
where snapshot_total.total_amount != source_total.total_amount
