-- spec (docs/facts/user_monthly_snapshot_fact.md section 9): "Transaction
-- count reconciliation" — sum(transaction_count) for the month must equal
-- count(*) of fact_transactions for that same month (customer_key = '-1'
-- excluded, matching this model's own filter). Scoped to only the current
-- batch month (not full history) — cheaper per run, and matches this
-- model's own insert_overwrite recompute scope, so it reuses the same
-- batch_logical_date() macro the model uses to pick that month.
--
-- TODO: silent pass on a missing partition. If the snapshot has no rows for
-- the batch month, sum(transaction_count) is NULL and `NULL != <n>` is NULL,
-- so no row is returned and the test passes — exactly the case it most needs
-- to catch. Wrap the aggregate in coalesce(..., 0) like the
-- fact_daily_transaction_trend reconciliation tests do. Same fix needed in the
-- sibling amount reconciliation test.

with snapshot_month as (

    select
        cast(date_format(last_day({{ batch_logical_date() }}), 'yyyyMMdd') as int)
            as month_end_date_key

),

snapshot_total as (

    select sum(transaction_count) as transaction_count
    from {{ ref('fact_user_monthly_snapshot') }}
    where month_end_date_key = (select month_end_date_key from snapshot_month)

),

source_total as (

    select count(*) as transaction_count
    from {{ ref('fact_transactions') }}
    where customer_key != '-1'
      and date_trunc('month', to_date(cast(date_key as string), 'yyyyMMdd'))
          = date_trunc('month', {{ batch_logical_date() }})

)

select
    snapshot_total.transaction_count as snapshot_transaction_count,
    source_total.transaction_count as source_transaction_count
from snapshot_total
cross join source_total
where snapshot_total.transaction_count != source_total.transaction_count
