{{ config(severity = 'warn') }}

-- spec (docs/facts/daily_transaction_trend_fact.md section 9): "Unknown MCC
-- share" — the share of transaction_count sitting in the mcc = '-1' bucket
-- must stay below 5%. Unlike the other two checks this one isn't about the
-- aggregation being correct, it's about upstream MCC resolution quality
-- degrading; the '-1' bucket is deliberately kept as valid data (section 6.2),
-- so nothing else would notice it growing.
--
-- Section 9 rates this High / "Alert", not pipeline-halting, and this project
-- has no alert channel yet (transactions_fact.md Open Question #9), so it runs
-- at severity warn: dbt reports it without failing the build.
--
-- Batch-day scope and batch_logical_date() macro as per the sibling
-- reconciliation tests. An empty partition yields total_count = 0 and is
-- filtered out rather than divided by — nothing to judge, and the count
-- reconciliation test is the one responsible for catching a missing partition.

with batch_day as (

    select cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key

),

partition_totals as (

    select
        date_key,
        sum(transaction_count) as total_count,
        sum(case when mcc = '-1' then transaction_count else 0 end) as unknown_count
    from {{ ref('fact_daily_transaction_trend') }}
    where date_key = (select date_key from batch_day)
    group by date_key

)

select
    date_key,
    unknown_count,
    total_count,
    cast(unknown_count * 100.0 / nullif(total_count, 0) as decimal(5, 2)) as unknown_mcc_share_pct
from partition_totals
where total_count > 0
  -- Integer form of "share >= 5%" — kept as multiplication so the threshold is
  -- exact rather than subject to decimal division rounding.
  and unknown_count * 100 >= total_count * 5
