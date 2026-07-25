{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['date_key'],
    )
}}

-- Derived reporting model (spec docs/metrics/merchant_error_daily_report.md):
-- materializes the business definition "Abnormal Error Rate (merchant) =
-- error rate over the trailing 30 days above 4.0%, among merchants with at
-- least 50 transactions in that window" (business spec section 4, Decision
-- #22/#23/#24) so BI never re-implements the formula. Answers success
-- criterion 6 directly.
--
-- Grain = 1 row per (date_key, merchant_id) (section 3): the trailing-30-day
-- error state of one merchant as of the END of one day. Composite PK, no
-- surrogate key, matching every other aggregate fact in the repo. merchant_id
-- is a degenerate dimension — there is no merchant dimension (Decision #21).
--
-- THE VOLUME FLOOR IS PART OF THE TABLE'S SCOPE, not an optional filter: only
-- merchants with transaction_count_30d >= var('abnormal_error_min_transaction_count')
-- get a row (~165 rows/day out of ~10.4k merchants). Consequence to know:
-- "merchant X is absent" is ambiguous — X may be clean, or X may be too small.
-- Distinguishing the two means going back to fact_daily_transaction_trend.
-- portfolio_error_rate_30d is deliberately computed BEFORE the floor, over
-- every merchant, so the baseline in this table is the real one.
--
-- THIS TABLE STORES ITS RATIO, unlike fact_daily_transaction_trend where
-- Decision #15 forbids it. That table is meant to be rolled up, so a stored
-- rate there would get averaged; this one is the FINAL CONSUMPTION grain — the
-- rate at merchant x 30-day window IS the metric. The roll-up risk is still
-- real and is fenced off by the mandatory aggregation warnings in the yml plus
-- the "Rate consistency" test (section 9, Decision Log 2026-07-25). Same
-- reasoning, opposite conclusion to rpt_card_portfolio.
--
-- THIS IS A SHORTLIST FILTER, NOT A VERDICT. At the floor (n = 50) a merchant
-- needs only 3 failures to exceed 4% against an expectation of 0.8, and the 95%
-- CI around a rate measured there is roughly +/- 6 percentage points
-- (abnormal_error_rate_calibration.md section 4). That is why
-- excess_failed_transactions_30d exists: it, not error_rate_30d, is the column
-- BI should order by.
--
-- AGGREGATING ACROSS mcc IS MANDATORY AND THE '-1' BUCKET MUST BE KEPT
-- (section 6, registry rule #6, business spec Decision #16). The source grain
-- is date x mcc x merchant, so one merchant can occupy several rows on the same
-- day. Missing another mcc row of the same merchant, or dropping the '-1'
-- bucket (transactions whose category never resolved — still that merchant's
-- transactions), understates the DENOMINATOR, inflates the error rate, and
-- flags an innocent merchant. That is the most dangerous fault this model can
-- have, and the reason the reconciliation test deliberately recomputes from
-- fact_transactions instead of from the trend fact.
--
-- ONE CODE PATH FOR BOTH BRANCHES (Decision Log 2026-07-25): jinja appears in
-- exactly two places — the spine_bounds CTE and the source_trend read filter.
-- Every other CTE is identical in full-refresh and incremental mode, so the two
-- modes cannot drift apart. The two jinja sites MUST agree on the same window.
-- A flat group-by incremental branch would be faster and shorter but would
-- create two expressions that have to keep matching each other forever — the
-- kind of drift tests miss because both branches "run green".
--
-- KNOWN COST (section 8.1 step 3, accepted): the spine runs at (merchant, mcc)
-- grain from the pair's first transaction day to last transaction day + 29, so
-- a merchant active across the full 10 years still produces ~3,590 spine rows
-- per pair. A full-refresh intermediate can therefore reach tens of millions of
-- rows for a ~590k-row output. Accepted to keep a single code path; the
-- incremental branch reads only 30 days and is cheap.
--
-- batch_logical_date() means the DATA day, not the day the batch runs: a T+1
-- run on D+1 must be passed `--vars '{batch_logical_date: <D>}'`, otherwise the
-- macro's current_date() default overwrites an empty D+1 partition and leaves D
-- untouched (same rule as every other insert_overwrite fact). NEVER pass the
-- legacy PySpark "process everything" sentinel 2036-01-01: dim_dates ends
-- 2035-12-31, so the window resolves to nothing and the run writes zero rows in
-- silence while the reconciliation check still passes with 0 = 0 (section 2).

with history_bounds as (

    -- Scanned unfiltered on purpose (section 8.1 step 1) — history_start_date
    -- is needed for window_day_count even on an incremental run, whose read
    -- filter below only sees 30 days. Iceberg answers this from partition
    -- metadata, so it is cheap.
    select
        to_date(cast(min(date_key) as string), 'yyyyMMdd') as history_start_date,
        to_date(cast(max(date_key) as string), 'yyyyMMdd') as history_end_date
    from {{ ref('fact_daily_transaction_trend') }}

),

-- JINJA SITE 1 of 2. Where the output starts and stops (section 8.2):
--   full-refresh -> the first day that actually has data through the last one.
--     Anchoring the end on batch_logical_date() instead would fall back to
--     current_date() with no --vars and generate years of empty rows against
--     historical data. The first 29 days ARE emitted, with a truncated
--     window_day_count (Decision Log 2026-07-25).
--   incremental  -> exactly the batch day, following the sibling reporting
--     models: a backfill of an older D must stop at D, and D does not have to
--     be a day with data.
spine_bounds as (

    select
        {% if is_incremental() %}
        {{ batch_logical_date() }} as output_start_date,
        {{ batch_logical_date() }} as spine_end_date,
        {% else %}
        history_start_date as output_start_date,
        history_end_date as spine_end_date,
        {% endif %}
        history_start_date
    from history_bounds

),

spine_window as (

    -- window_start_date is how far back the spines have to reach so the
    -- trailing frames evaluated on the OUTPUT days are complete: 29 days before
    -- the first output day, never earlier than the first day with data.
    select
        output_start_date,
        spine_end_date,
        history_start_date,
        greatest(date_sub(output_start_date, 29), history_start_date)
            as window_start_date
    from spine_bounds

),

-- The days this run emits partitions for. THE single place that narrows the
-- output scope (section 8.1 step 9): everything downstream inner-joins it, so
-- there is no per-CTE `where date_key = <batch day>` that could get out of sync.
output_dates as (

    select
        dd.date_key,
        dd.full_date
    from {{ ref('dim_dates') }} as dd
    -- spine_window is one row, so this cross join cannot fan out.
    cross join spine_window as sw
    where dd.full_date between sw.output_start_date and sw.spine_end_date

),

source_trend as (

    select
        date_key,
        mcc,
        merchant_id,
        transaction_count,
        failed_transaction_count,
        total_spend_amount
    from {{ ref('fact_daily_transaction_trend') }}

    -- JINJA SITE 2 of 2 — must stay in step with spine_window.window_start_date
    -- above. Written as literals rather than a scalar subquery so the predicate
    -- reaches the Iceberg scan as a partition filter. Pushing the filter down IS
    -- valid here, unlike in fact_customer_activity_daily: every output day needs
    -- exactly 30 days of trend rows and nothing else — no column reaches back
    -- over full history (section 8.1 step 2).
    --
    -- NO sentinel filter, deliberately: the mcc = '-1' bucket is kept (section 6
    -- above), which is also what makes the exact reconciliation against
    -- fact_transactions possible.
    {% if is_incremental() %}
    where date_key
        between cast(date_format(date_sub({{ batch_logical_date() }}, 29), 'yyyyMMdd') as int)
            and cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int)
    {% endif %}

),

-- The trend fact is already unique per (date_key, mcc, merchant_id); this group
-- by is defensive and does the date_key -> date conversion the spine joins on.
pair_daily as (

    select
        merchant_id,
        mcc,
        to_date(cast(date_key as string), 'yyyyMMdd') as transaction_date,
        sum(transaction_count) as daily_transaction_count,
        sum(failed_transaction_count) as daily_failed_count,
        sum(total_spend_amount) as daily_spend_amount
    from source_trend
    group by merchant_id, mcc, to_date(cast(date_key as string), 'yyyyMMdd')

),

pair_bounds as (

    select
        merchant_id,
        mcc,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from pair_daily
    group by merchant_id, mcc

),

-- One row per (merchant, mcc) per calendar day. dim_dates is generated gap-free
-- and this range is CONTIGUOUS (first transaction day .. last transaction day
-- + 29, no per-gap cutting), which is what makes the ROWS frame below
-- equivalent to a 30-calendar-day window. A range join of spine x transactions
-- is deliberately avoided: Spark turns non-equi joins into a per-merchant
-- cartesian (section 8.1 step 4).
--
-- Cutting the tail at last_transaction_date + 29 is EXACT, not an
-- approximation: past that point the trailing window of that pair is empty, so
-- transaction_count_30d = 0 < floor and the row would be dropped anyway.
--
-- The lower bound takes greatest(first transaction day, window_start_date) so
-- the same expression works in both branches: on an incremental run every
-- transaction older than window_start_date was already excluded by the read
-- filter, so the rows missing here could not have contributed to the frame.
pair_spine as (

    select
        dd.date_key,
        dd.full_date,
        pb.merchant_id,
        pb.mcc
    from {{ ref('dim_dates') }} as dd
    -- Both bounds of the range depend on pair_bounds AND on spine_window, so
    -- the predicates live in the where clause rather than in a join condition;
    -- spine_window is one row, so it adds no fan-out of its own.
    cross join pair_bounds as pb
    cross join spine_window as sw
    where dd.full_date >= greatest(pb.first_transaction_date, sw.window_start_date)
      and dd.full_date <= least(date_add(pb.last_transaction_date, 29), sw.spine_end_date)

),

pair_daily_on_spine as (

    select
        ps.date_key,
        ps.full_date,
        ps.merchant_id,
        ps.mcc,
        coalesce(pd.daily_transaction_count, 0) as daily_transaction_count,
        coalesce(pd.daily_failed_count, 0) as daily_failed_count,
        coalesce(pd.daily_spend_amount, 0) as daily_spend_amount
    from pair_spine as ps
    left join pair_daily as pd
        on ps.merchant_id = pd.merchant_id
       and ps.mcc = pd.mcc
       and ps.full_date = pd.transaction_date

),

pair_windowed as (

    select
        date_key,
        full_date,
        merchant_id,
        mcc,
        -- Trailing (D - 29, D]: 29 preceding rows plus the current one = 30
        -- calendar days, current day included — the same convention as the
        -- 90-day windows of the sibling reporting models (section 3).
        sum(daily_transaction_count) over w_trailing_30d as transaction_count_30d,
        sum(daily_failed_count) over w_trailing_30d as failed_transaction_count_30d,
        sum(daily_spend_amount) over w_trailing_30d as total_spend_amount_30d
    from pair_daily_on_spine
    -- A named window rather than three inline frames so the three measures can
    -- never silently drift onto different windows.
    window
        w_trailing_30d as (
            partition by merchant_id, mcc
            order by full_date
            rows between 29 preceding and current row
        )

),

-- Narrowed to the days this run emits; the preceding 29 days existed only to
-- fill the frames. Done once, here, and inherited by everything below.
pair_windowed_output as (

    select
        pw.date_key,
        pw.full_date,
        pw.merchant_id,
        pw.mcc,
        pw.transaction_count_30d,
        pw.failed_transaction_count_30d,
        pw.total_spend_amount_30d
    from pair_windowed as pw
    inner join output_dates as od
        on pw.date_key = od.date_key

),

-- Section 8.1 step 5: sum of rolling sums = rolling sum of the sum, so
-- collapsing mcc here is valid for every count/amount column.
merchant_windowed as (

    select
        date_key,
        full_date,
        merchant_id,
        sum(transaction_count_30d) as transaction_count_30d,
        sum(failed_transaction_count_30d) as failed_transaction_count_30d,
        sum(total_spend_amount_30d) as total_spend_amount_30d,
        -- How approximate primary_mcc is: 1 means exact, > 1 means a
        -- multi-category merchant carrying a single label.
        sum(case when transaction_count_30d > 0 then 1 else 0 end) as distinct_mcc_count
    from pair_windowed_output
    group by date_key, full_date, merchant_id

),

-- The merchant's busiest mcc WITHIN THE WINDOW, ties broken by the smallest mcc
-- as a string so the result is deterministic (section 5.1). row_number rather
-- than max_by because max_by has no defined tie-break. mcc values with a zero
-- rolling count are excluded — they are pairs still inside their +29 tail.
merchant_primary_mcc as (

    select
        date_key,
        merchant_id,
        mcc as primary_mcc
    from (
        select
            date_key,
            merchant_id,
            mcc,
            row_number() over (
                partition by date_key, merchant_id
                order by transaction_count_30d desc, mcc asc
            ) as mcc_rank
        from pair_windowed_output
        where transaction_count_30d > 0
    ) as ranked
    where mcc_rank = 1

),

-- Section 8.1 step 6: the portfolio baseline is computed over EVERY merchant
-- and BEFORE the volume floor is applied. Computing it after the floor would
-- give an artificially higher baseline and shrink every excess systematically —
-- and it is not how calibration measured the 1.609% mark (over all 10,433
-- merchants, not the ~165 qualified ones). The "Baseline uniform per day" test
-- exists to catch exactly that mistake.
portfolio_daily as (

    select
        transaction_date,
        sum(daily_transaction_count) as daily_transaction_count,
        sum(daily_failed_count) as daily_failed_count
    from pair_daily
    group by transaction_date

),

portfolio_spine as (

    select
        dd.date_key,
        dd.full_date,
        coalesce(pfd.daily_transaction_count, 0) as daily_transaction_count,
        coalesce(pfd.daily_failed_count, 0) as daily_failed_count
    from {{ ref('dim_dates') }} as dd
    cross join spine_window as sw
    left join portfolio_daily as pfd
        on dd.full_date = pfd.transaction_date
    where dd.full_date between sw.window_start_date and sw.spine_end_date

),

portfolio_windowed as (

    select
        date_key,
        -- Same frame as the merchant side; one global series, so no partition by.
        sum(daily_transaction_count) over w_trailing_30d as portfolio_transaction_count_30d,
        sum(daily_failed_count) over w_trailing_30d as portfolio_failed_count_30d
    from portfolio_spine
    window
        w_trailing_30d as (
            order by full_date
            rows between 29 preceding and current row
        )

),

portfolio_rate as (

    select
        pw.date_key,
        -- The zero branch cannot surface in the output: a day with no
        -- transactions portfolio-wide has no merchant above the floor either.
        -- It exists so the column can be NOT NULL by construction rather than
        -- by luck.
        case
            when pw.portfolio_transaction_count_30d > 0
                then cast(pw.portfolio_failed_count_30d / pw.portfolio_transaction_count_30d as decimal(9, 6))
            else cast(0 as decimal(9, 6))
        end as portfolio_error_rate_30d
    from portfolio_windowed as pw
    inner join output_dates as od
        on pw.date_key = od.date_key

),

-- Section 8.1 step 7. The floor is part of the table's scope, not a filter a
-- consumer may undo — the "Volume floor enforced" test asserts it holds on
-- every row.
qualified as (

    select
        mw.date_key,
        mw.full_date,
        mw.merchant_id,
        pm.primary_mcc,
        mw.distinct_mcc_count,
        mw.transaction_count_30d,
        mw.failed_transaction_count_30d,
        mw.total_spend_amount_30d,
        pr.portfolio_error_rate_30d
    from merchant_windowed as mw
    -- Inner joins are safe: every row past the floor has transaction_count_30d
    -- >= 50 > 0, so at least one mcc has a positive rolling count and
    -- merchant_primary_mcc always matches; portfolio_rate has exactly one row
    -- per output day.
    inner join merchant_primary_mcc as pm
        on mw.date_key = pm.date_key
       and mw.merchant_id = pm.merchant_id
    inner join portfolio_rate as pr
        on mw.date_key = pr.date_key
    where mw.transaction_count_30d >= {{ var('abnormal_error_min_transaction_count') }}

),

-- expected_failed_transactions_30d is rounded to its stored decimal(12,2) HERE
-- so that excess = failed - expected below is computed from the SAME value the
-- table stores. That makes the "Excess consistency" check an exact equality
-- instead of a tolerance comparison.
computed as (

    select
        date_key,
        full_date,
        merchant_id,
        primary_mcc,
        distinct_mcc_count,
        transaction_count_30d,
        failed_transaction_count_30d,
        total_spend_amount_30d,
        portfolio_error_rate_30d,
        cast(failed_transaction_count_30d / transaction_count_30d as decimal(9, 6))
            as error_rate_30d,
        cast(transaction_count_30d * portfolio_error_rate_30d as decimal(12, 2))
            as expected_failed_transactions_30d
    from qualified

)

select
    c.date_key,
    c.merchant_id,
    c.primary_mcc,
    cast(c.distinct_mcc_count as int) as distinct_mcc_count,
    -- 30 everywhere except the first 29 days of history, where the window is
    -- truncated (section 5.1). Rows below 30 must not be compared against
    -- full-window rows. Deliberately NOT clamped, same as rpt_card_portfolio:
    -- backfilling a batch day that precedes the first data day is outside this
    -- model's contract, and a value that trips the 1..30 range test is a louder
    -- failure than a silently clamped one.
    cast(
        datediff(
            c.full_date,
            greatest(date_sub(c.full_date, 29), sw.history_start_date)
        ) + 1 as int
    ) as window_day_count,
    cast(c.transaction_count_30d as int) as transaction_count_30d,
    cast(c.failed_transaction_count_30d as int) as failed_transaction_count_30d,
    c.error_rate_30d,
    c.portfolio_error_rate_30d,
    c.expected_failed_transactions_30d,
    cast(
        c.failed_transaction_count_30d - c.expected_failed_transactions_30d
        as decimal(12, 2)
    ) as excess_failed_transactions_30d,
    cast(c.total_spend_amount_30d as decimal(18, 2)) as total_spend_amount_30d,
    -- The two parameters are carried onto every row (Decision Log 2026-07-25):
    -- they have already changed once (5% -> 4.0%, Decision #24), so historical
    -- rows must be able to say which parameters produced them instead of making
    -- the reader dig through git. Changing the vars does NOT rewrite history —
    -- that needs a --full-refresh, on purpose.
    cast({{ var('abnormal_error_rate_threshold') }} as decimal(5, 4))
        as applied_error_rate_threshold,
    cast({{ var('abnormal_error_min_transaction_count') }} as int)
        as applied_min_transaction_count,
    -- The Abnormal Error Rate flag of business spec section 4. The volume-floor
    -- half of that definition is already in the table's scope (section 3), so
    -- this flag carries only the threshold half. Compared on the two STORED
    -- columns, which is what makes the "Flag consistency" test exact.
    c.error_rate_30d > cast({{ var('abnormal_error_rate_threshold') }} as decimal(5, 4))
        as is_abnormal_error_rate
from computed as c
-- spine_window is one row.
cross join spine_window as sw
