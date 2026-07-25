{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['date_key'],
    )
}}

-- Derived reporting fact (spec docs/facts/customer_activity_daily_fact.md):
-- materializes the business definition "Active Customer / Active Card = at
-- least one transaction in the trailing 90 calendar days" (business spec
-- section 4) so BI never re-implements the formula. First model of the
-- reporting layer (Decision Log 2026-07-23).
--
-- Grain = 1 row per (date_key, customer_id) (section 3): every calendar day
-- from a customer's first transaction onward, including days with no activity.
-- The grain is the NATURAL key, not the SCD2 version — activity belongs to the
-- customer, not to a dimension version — while customer_key is carried as-of
-- end of day so BI gets point-in-time segments from one plain equality join
-- (Decision Log 2026-07-23).
--
-- Load strategy: insert_overwrite by date_key (section 8.1). Unlike the sibling
-- fact-of-facts, the is_incremental() day filter CANNOT sit in the source CTE:
-- every output day needs the 90 days of activity preceding it, plus full
-- history for last_transaction_date_key and for each customer's first
-- transaction date. The filter therefore sits after the window CTE
-- (batch_partition below), and dynamic partition overwrite rewrites only the
-- single partition present in the output. An incremental run does compute
-- windows for every day and keep one; the full fact_transactions scan is
-- unavoidable regardless, and the data is small (thousands of customers x days)
-- — the alternative, a separate incremental-only code path, would let the two
-- modes drift apart.
--
-- batch_logical_date() means the DATA day, not the day the batch runs: a T+1
-- run on D+1 must be passed `--vars '{batch_logical_date: <D>}'`, otherwise the
-- macro's current_date() default overwrites an empty D+1 partition and leaves D
-- untouched (same rule as fact_daily_transaction_trend).

with source_transactions as (

    select
        customer_key,
        card_key,
        date_key
    from {{ ref('fact_transactions') }}

    -- customer_key = '-1' (Unknown) is dropped entirely, matching
    -- fact_user_monthly_snapshot rather than fact_daily_transaction_trend:
    -- a per-customer activity table has no reporting meaning for an
    -- unresolved customer, and there is no customer_id to build a spine for
    -- (section 6, Decision Log 2026-07-24). The known consequence — totals
    -- here don't tie back to fact_transactions exactly — is documented in
    -- section 3.
    where customer_key != '-1'

    -- Deliberately NO is_incremental() filter here; see the header comment.

),

-- Collapse to (customer_id, transaction day) BEFORE the spine join: daily
-- transaction count plus the set of distinct cards used that day.
--
-- Cards are counted on the NATURAL key card_id, not the surrogate card_key: a
-- card whose SCD2 attributes change mid-window carries two card_keys but is
-- still one card (Decision Log 2026-07-24). card_key = '-1' fails the join
-- predicate, leaving card_id NULL, and collect_set drops NULLs — so sentinel
-- cards never enter the distinct count while their transactions still count
-- towards transaction_count_90d (section 5.1).
--
-- Both joins are exact surrogate matches (customer_key/card_key are unique in
-- their dimensions), so neither can fan out; the inner join to dim_customers
-- cannot drop rows either, since fact_transactions' relationships test
-- guarantees every non-sentinel key resolves.
customer_daily as (

    select
        dc.customer_id,
        to_date(cast(t.date_key as string), 'yyyyMMdd') as transaction_date,
        count(*) as daily_transaction_count,
        collect_set(dca.card_id) as daily_card_ids
    from source_transactions as t
    inner join {{ ref('dim_customers') }} as dc
        on t.customer_key = dc.customer_key
    left join {{ ref('dim_cards') }} as dca
        on t.card_key = dca.card_key
       and t.card_key != '-1'
    group by dc.customer_id, to_date(cast(t.date_key as string), 'yyyyMMdd')

),

customer_first_transaction as (

    select
        customer_id,
        min(transaction_date) as first_transaction_date
    from customer_daily
    group by customer_id

),

-- Where the spine stops (section 3, Decision Log 2026-07-24):
--   full-refresh -> the last day that actually has a transaction. Anchoring on
--     batch_logical_date() instead would fall back to current_date() when no
--     --vars is passed and generate years of empty rows against historical data.
--   incremental  -> the batch day itself, NOT the last day with data. A day D
--     with no transactions anywhere must still get its full partition (every
--     customer whose first transaction is <= D, measures reflecting the
--     trailing window), and a backfill of an older D must stop at D.
spine_end as (

    {% if is_incremental() %}
    select {{ batch_logical_date() }} as spine_end_date
    {% else %}
    select max(transaction_date) as spine_end_date from customer_daily
    {% endif %}

),

-- One row per customer per calendar day. dim_dates is generated gap-free, so
-- this spine has exactly one row per (customer, day) with no holes — which is
-- what makes the ROWS window frames below equivalent to calendar-day windows.
--
-- No transaction can be lost by this join: fact_transactions already drops rows
-- whose date_key doesn't resolve against dim_dates, so every transaction day is
-- present in the spine's date range.
spine as (

    select
        dd.date_key,
        dd.full_date,
        cft.customer_id
    from {{ ref('dim_dates') }} as dd
    inner join customer_first_transaction as cft
        on dd.full_date >= cft.first_transaction_date
    where dd.full_date <= (select spine_end_date from spine_end)

),

daily_activity as (

    select
        s.date_key,
        s.full_date,
        s.customer_id,
        coalesce(cd.daily_transaction_count, 0) as daily_transaction_count,
        -- Left NULL rather than coalesced to an empty array: collect_list below
        -- skips NULLs, which is exactly the wanted behaviour for a day with no
        -- transactions.
        cd.daily_card_ids
    from spine as s
    left join customer_daily as cd
        on s.customer_id = cd.customer_id
       and s.full_date = cd.transaction_date

),

windowed as (

    select
        date_key,
        full_date,
        customer_id,
        -- Trailing (D - 89, D]: 89 preceding rows plus the current one = 90
        -- calendar days, current day included (Decision Log 2026-07-23).
        sum(daily_transaction_count) over w_trailing_90d as transaction_count_90d,
        -- Spark has no count(distinct) over a window, so the per-day card sets
        -- are collected, flattened and de-duplicated instead. The sets are tiny
        -- (cards per customer per day). An all-sentinel or no-transaction frame
        -- yields an empty array, never NULL, so size() cannot return -1.
        size(array_distinct(flatten(collect_list(daily_card_ids) over w_trailing_90d)))
            as active_card_count_90d,
        -- Recency over FULL history, not limited to 90 days (section 5.1).
        -- Never NULL in practice: the spine starts on the first transaction day,
        -- so every row has at least one active day at or before it.
        max(case when daily_transaction_count > 0 then full_date end) over w_history
            as last_transaction_date
    from daily_activity
    -- Named windows rather than inline frames so the two trailing measures can
    -- never silently drift onto different windows.
    window
        w_trailing_90d as (
            partition by customer_id
            order by full_date
            rows between 89 preceding and current row
        ),
        w_history as (
            partition by customer_id
            order by full_date
            rows between unbounded preceding and current row
        )

),

batch_partition as (

    select *
    from windowed

    -- The single-day filter belongs HERE, after the windows have been computed
    -- over full history — see the header comment.
    {% if is_incremental() %}
    where date_key = cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int)
    {% endif %}

)

select
    bp.date_key,
    bp.customer_id,
    -- As-of END of day D (section 7). With [from, to) validity, "end of day D"
    -- is the instant just before midnight of D+1, hence the comparison against
    -- D+1 rather than D. The strict `<` on effective_from_date matters: a
    -- version starting exactly at midnight of D+1 did not exist during day D
    -- and must not win. dim_customers' interval coverage/no-overlap tests
    -- guarantee exactly one match, so this join cannot fan out and the '-1'
    -- fallback should never fire — the yml watches it at severity warn
    -- (section 9 "Customer key resolution").
    coalesce(dc.customer_key, '-1') as customer_key,
    bp.transaction_count_90d > 0 as is_active_90d,
    cast(bp.active_card_count_90d as int) as active_card_count_90d,
    cast(bp.transaction_count_90d as int) as transaction_count_90d,
    cast(date_format(bp.last_transaction_date, 'yyyyMMdd') as int)
        as last_transaction_date_key
from batch_partition as bp
left join {{ ref('dim_customers') }} as dc
    on bp.customer_id = dc.customer_id
   and dc.effective_from_date < cast(date_add(bp.full_date, 1) as timestamp)
   and dc.effective_to_date >= cast(date_add(bp.full_date, 1) as timestamp)
