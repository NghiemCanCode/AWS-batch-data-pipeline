{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['date_key'],
    )
}}

-- Derived reporting model (spec docs/metrics/card_portfolio_report.md):
-- materializes the whole of Dashboard C — issued vs active cards, Chip
-- Adoption Rate, card reissue distribution, and transaction success rate by
-- card type — so BI never re-implements the joins behind them. Answers success
-- criterion 5 directly (Decision Log 2026-07-25, business spec Decision #26).
--
-- Grain = 1 row per (date_key, chip_segment, card_brand) (section 3): the state
-- of one card segment as of the END of one day. Composite PK, no surrogate key,
-- matching every other aggregate fact in the repo.
--
-- COUNTS ONLY, NO STORED RATIOS (Decision Log 2026-07-25). Chip adoption and
-- success rate are computed downstream from the count columns, because this
-- grain is meant to be rolled up (collapse card_brand for portfolio-wide chip
-- adoption, collapse chip_segment to compare brands) and any ratio stored at a
-- to-be-rolled-up grain gets silently averaged (registry rule #1). This is the
-- deliberate opposite of rpt_merchant_error_daily, which is a final consumption
-- grain and therefore does store its rate.
--
-- Two sides, full-outer-joined at the end:
--   issued side — point-in-time SCD2 count from dim_cards (registry rule #5),
--     NOT is_current repeated per day, which would stamp today's attributes on
--     every past day and flatten the chip adoption line forever.
--   active side — distinct cards and transaction counts over the trailing
--     90-day window (D - 89, D], the same window and the same convention as
--     fact_customer_activity_daily's Active Card definition.
-- Both sides classify a card by the version in effect at the END of day D, so
-- every card sits in exactly one segment per day: issued and active share a
-- denominator, and summing across segments double-counts nothing.
--
-- KNOWN LIMITATION (section 3.1): dim_cards backdates each card's version 1 to
-- 1900-01-01 and the synthetic source is static (one version per card), so
-- issued_card_count is a CONSTANT across date_key and the chip adoption line is
-- flat. That is not a model bug — the point-in-time logic is correct and will
-- produce a real curve as soon as the source can generate attribute changes.
-- The active-side columns are not affected: transactions have a real time
-- distribution, so those lines have real shape.
--
-- ONE CODE PATH FOR BOTH BRANCHES (Decision Log 2026-07-25): jinja appears in
-- exactly two places — the spine_bounds CTE and the source_transactions read
-- filter. Every other CTE is identical in full-refresh and incremental mode, so
-- the two modes cannot drift apart. The two jinja sites MUST agree on the same
-- window; the read filter is written out as a literal predicate only so Iceberg
-- can prune partitions instead of scanning all history (section 8.1 step 4).
-- Unlike fact_customer_activity_daily, pushing the filter down IS valid here:
-- every output day needs exactly 90 days of transactions and nothing else — no
-- column reaches back over full history.
--
-- batch_logical_date() means the DATA day, not the day the batch runs: a T+1
-- run on D+1 must be passed `--vars '{batch_logical_date: <D>}'`, otherwise the
-- macro's current_date() default overwrites an empty D+1 partition and leaves D
-- untouched (same rule as every other insert_overwrite fact). NEVER pass the
-- legacy PySpark "process everything" sentinel 2036-01-01: dim_dates ends
-- 2035-12-31, so the spine resolves to nothing and the run writes zero rows in
-- silence (section 2).

with history_bounds as (

    -- Scanned unfiltered on purpose (section 8.1 step 1) — history_start_date
    -- is needed for window_day_count even on an incremental run, whose read
    -- filter below only sees 90 days. Iceberg answers this from partition
    -- metadata, so it is cheap.
    select
        to_date(cast(min(date_key) as string), 'yyyyMMdd') as history_start_date,
        to_date(cast(max(date_key) as string), 'yyyyMMdd') as history_end_date
    from {{ ref('fact_transactions') }}

),

-- JINJA SITE 1 of 2. Where the output starts and stops (section 3):
--   full-refresh -> the first day that actually has a transaction (NOT
--     1900-01-01: the dim_cards backdate is a join technique, not a business
--     event, and spining from 1900 would emit ~40k days of repeated values)
--     through the last day that actually has one. Anchoring the end on
--     batch_logical_date() instead would fall back to current_date() with no
--     --vars and generate years of empty rows against historical data.
--   incremental  -> exactly the batch day, following fact_customer_activity_daily:
--     a day D with no transactions anywhere must still get its partition, and a
--     backfill of an older D must stop at D.
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

    -- window_start_date is how far back the card spine has to reach so that the
    -- trailing frames evaluated on the OUTPUT days are complete: 89 days before
    -- the first output day, never earlier than the first day with data.
    select
        output_start_date,
        spine_end_date,
        history_start_date,
        greatest(date_sub(output_start_date, 89), history_start_date)
            as window_start_date
    from spine_bounds

),

-- The days this run emits partitions for. THE single place that narrows the
-- output scope: both sides inner-join it, so there is no per-side
-- `where date_key = <batch day>` that could get out of sync (section 8.1 step 7).
output_dates as (

    select
        dd.date_key,
        dd.full_date
    from {{ ref('dim_dates') }} as dd
    -- spine_window is one row, so this cross join cannot fan out.
    cross join spine_window as sw
    where dd.full_date between sw.output_start_date and sw.spine_end_date

),

source_transactions as (

    select
        card_key,
        date_key,
        is_error
    from {{ ref('fact_transactions') }}

    -- JINJA SITE 2 of 2 — must stay in step with spine_window.window_start_date
    -- above. Written as literals rather than a scalar subquery so the predicate
    -- reaches the Iceberg scan as a partition filter.
    --
    -- No sentinel filter: transactions on an unresolved card (card_key = '-1')
    -- are KEPT and land in the (UNKNOWN, UNKNOWN) bucket, following the
    -- fact_daily_transaction_trend precedent (business spec Decision #16) and
    -- deliberately NOT fact_customer_activity_daily, which drops them. A failed
    -- transaction on an unresolved card is still a real failed transaction;
    -- dropping it would understate the success-rate denominator and break the
    -- exact reconciliation against fact_transactions (section 6, Decision Log
    -- 2026-07-25).
    {% if is_incremental() %}
    where date_key
        between cast(date_format(date_sub({{ batch_logical_date() }}, 89), 'yyyyMMdd') as int)
            and cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int)
    {% endif %}

),

-- Collapse to (card_id, transaction day) BEFORE any spine join.
--
-- Cards are identified by the NATURAL key card_id, not the surrogate card_key:
-- a card whose SCD2 attributes change mid-window carries two card_keys but is
-- still one card (same reason as fact_customer_activity_daily). The join is an
-- exact surrogate match against a unique key, so it cannot fan out.
--
-- Both seed members are excluded from the join, not just '-1': dim_cards' '-2'
-- row carries card_id = 'NOT APPLICABLE', which looks like a real natural key
-- and would enter the distinct card count as a phantom card. Rows that match
-- neither keep card_id NULL and become the sentinel branch below.
card_daily as (

    select
        dca.card_id,
        to_date(cast(t.date_key as string), 'yyyyMMdd') as transaction_date,
        count(*) as daily_transaction_count,
        sum(case when t.is_error = false then 1 else 0 end) as daily_successful_count,
        sum(case when t.is_error = true then 1 else 0 end) as daily_failed_count
    from source_transactions as t
    left join {{ ref('dim_cards') }} as dca
        on t.card_key = dca.card_key
       and t.card_key not in ('-1', '-2')
    group by dca.card_id, to_date(cast(t.date_key as string), 'yyyyMMdd')

),

card_first_activity as (

    select
        card_id,
        min(transaction_date) as first_activity_date
    from card_daily
    where card_id is not null
    group by card_id

),

-- One row per card per calendar day. dim_dates is generated gap-free, so this
-- spine has no holes — which is what makes the ROWS frame below equivalent to a
-- calendar-day window. A range join of spine x transactions is deliberately
-- avoided: Spark turns non-equi joins into a per-card cartesian (section 8.1
-- step 5, same reasoning as fact_customer_activity_daily).
--
-- Starting a card's spine at window_start_date rather than at its first
-- transaction is safe and is what keeps the two branches equal: the frame at an
-- output day looks back at most 89 rows, and on an incremental run every
-- transaction older than that was already excluded by the read filter, so the
-- rows that are missing here could not have contributed anyway.
card_spine as (

    select
        dd.date_key,
        dd.full_date,
        cfa.card_id
    from {{ ref('dim_dates') }} as dd
    inner join card_first_activity as cfa
        on dd.full_date >= cfa.first_activity_date
    cross join spine_window as sw
    where dd.full_date >= sw.window_start_date
      and dd.full_date <= sw.spine_end_date

),

card_daily_on_spine as (

    select
        cs.date_key,
        cs.full_date,
        cs.card_id,
        coalesce(cd.daily_transaction_count, 0) as daily_transaction_count,
        coalesce(cd.daily_successful_count, 0) as daily_successful_count,
        coalesce(cd.daily_failed_count, 0) as daily_failed_count
    from card_spine as cs
    left join card_daily as cd
        on cs.card_id = cd.card_id
       and cs.full_date = cd.transaction_date

),

card_windowed as (

    select
        date_key,
        full_date,
        card_id,
        -- Trailing (D - 89, D]: 89 preceding rows plus the current one = 90
        -- calendar days, current day included — the same width and the same
        -- convention as the Active Card definition (section 3).
        sum(daily_transaction_count) over w_trailing_90d as transaction_count_90d,
        sum(daily_successful_count) over w_trailing_90d as successful_transaction_count_90d,
        sum(daily_failed_count) over w_trailing_90d as failed_transaction_count_90d
    from card_daily_on_spine
    -- A named window rather than three inline frames so the three measures can
    -- never silently drift onto different windows.
    window
        w_trailing_90d as (
            partition by card_id
            order by full_date
            rows between 89 preceding and current row
        )

),

-- Assign each card to the segment of the version in effect at the END of day D
-- (section 7, Decision Log 2026-07-25). With [from, to) validity, "end of day
-- D" is the instant just before midnight of D+1, hence the comparison against
-- D+1 rather than D; the strict `<` on effective_from_date matters, since a
-- version starting exactly at midnight of D+1 did not exist during day D.
-- dim_cards' interval coverage/no-overlap tests guarantee exactly one match, so
-- this join cannot fan out and the UNKNOWN fallback should never fire for a
-- real card.
--
-- The alternative — classifying each TRANSACTION by the card version it was
-- made under — is more faithful per transaction but would make one card active
-- in TWO segments when its attributes change mid-window, creating a fresh
-- double-counting trap. With the current static source both give identical
-- results; the one with fewer traps was chosen.
card_segment as (

    select
        cw.date_key,
        case
            when dca.has_chip = true then 'CHIP'
            when dca.has_chip = false then 'NON_CHIP'
            else 'UNKNOWN'
        end as chip_segment,
        coalesce(dca.card_brand, 'UNKNOWN') as card_brand,
        cw.transaction_count_90d,
        cw.successful_transaction_count_90d,
        cw.failed_transaction_count_90d
    from card_windowed as cw
    -- Narrows the windowed rows down to the days this run emits; the preceding
    -- 89 days existed only to fill the frames.
    inner join output_dates as od
        on cw.date_key = od.date_key
    left join {{ ref('dim_cards') }} as dca
        on cw.card_id = dca.card_id
       and dca.effective_from_date < cast(date_add(cw.full_date, 1) as timestamp)
       and dca.effective_to_date >= cast(date_add(cw.full_date, 1) as timestamp)

),

active_side_cards as (

    select
        date_key,
        chip_segment,
        card_brand,
        -- card_segment holds exactly one row per (card, day), so a plain
        -- conditional sum already IS the distinct card count — no count(distinct)
        -- needed, and no risk of one card being counted in two segments.
        sum(case when transaction_count_90d > 0 then 1 else 0 end) as active_card_count_90d,
        sum(transaction_count_90d) as transaction_count_90d,
        sum(successful_transaction_count_90d) as successful_transaction_count_90d,
        sum(failed_transaction_count_90d) as failed_transaction_count_90d
    from card_segment
    group by date_key, chip_segment, card_brand

),

-- Transactions whose card never resolved. They have no card_id, so they cannot
-- ride the card spine and cannot enter active_card_count_90d — but they DO
-- count towards the transaction counts of the (UNKNOWN, UNKNOWN) bucket
-- (section 6). A row with transaction_count_90d > 0 and issued = active = 0 is
-- the normal shape of that bucket, not an error.
sentinel_daily as (

    select
        transaction_date,
        daily_transaction_count,
        daily_successful_count,
        daily_failed_count
    from card_daily
    where card_id is null

),

sentinel_spine as (

    select
        dd.date_key,
        dd.full_date
    from {{ ref('dim_dates') }} as dd
    cross join spine_window as sw
    where dd.full_date between sw.window_start_date and sw.spine_end_date

),

sentinel_windowed as (

    select
        ss.date_key,
        -- Same frame as the card side; one global series, so no partition by.
        sum(coalesce(sd.daily_transaction_count, 0)) over w_trailing_90d
            as transaction_count_90d,
        sum(coalesce(sd.daily_successful_count, 0)) over w_trailing_90d
            as successful_transaction_count_90d,
        sum(coalesce(sd.daily_failed_count, 0)) over w_trailing_90d
            as failed_transaction_count_90d
    from sentinel_spine as ss
    left join sentinel_daily as sd
        on ss.full_date = sd.transaction_date
    window
        w_trailing_90d as (
            order by ss.full_date
            rows between 89 preceding and current row
        )

),

-- Union rather than a separate output row: a REAL card with has_chip NULL (if
-- one ever appears) also lands in (UNKNOWN, UNKNOWN) and must share the bucket
-- with the sentinel transactions rather than collide with them at the grain.
active_side_union as (

    select
        date_key,
        chip_segment,
        card_brand,
        active_card_count_90d,
        transaction_count_90d,
        successful_transaction_count_90d,
        failed_transaction_count_90d
    from active_side_cards

    union all

    select
        sw2.date_key,
        'UNKNOWN' as chip_segment,
        'UNKNOWN' as card_brand,
        0 as active_card_count_90d,
        sw2.transaction_count_90d,
        sw2.successful_transaction_count_90d,
        sw2.failed_transaction_count_90d
    from sentinel_windowed as sw2
    inner join output_dates as od
        on sw2.date_key = od.date_key

),

active_side as (

    select
        date_key,
        chip_segment,
        card_brand,
        sum(active_card_count_90d) as active_card_count_90d,
        sum(transaction_count_90d) as transaction_count_90d,
        sum(successful_transaction_count_90d) as successful_transaction_count_90d,
        sum(failed_transaction_count_90d) as failed_transaction_count_90d
    from active_side_union
    group by date_key, chip_segment, card_brand

),

-- Point-in-time issued count: the versions in effect at the end of day D, one
-- per card_id thanks to dim_cards' coverage/no-overlap tests. An as-of range
-- join with no equality predicate, so Spark runs it as a broadcast nested loop
-- — dim_cards is a few thousand rows, which makes that the cheap and readable
-- option; a delta + cumulative-sum formulation would be cheaper still but adds
-- an indirection where an off-by-one day at the boundary is easy to miss.
--
-- Both seed members are excluded: they are referential-integrity placeholders,
-- not issued cards (section 5.1).
issued_side as (

    select
        od.date_key,
        case
            when dca.has_chip = true then 'CHIP'
            when dca.has_chip = false then 'NON_CHIP'
            else 'UNKNOWN'
        end as chip_segment,
        coalesce(dca.card_brand, 'UNKNOWN') as card_brand,
        count(*) as issued_card_count,
        -- Reissue distribution as three buckets plus the total (section 5.1).
        -- The boundaries 0 / 1 / 2+ are hardcoded on purpose, not exposed as
        -- dbt vars: moving them changes what the COLUMNS MEAN and has to go
        -- through the spec, unlike rpt_merchant_error_daily's tunable threshold.
        -- Cards with a NULL num_card_issue fall in no bucket, which is why the
        -- three buckets sum to <= issued_card_count rather than to it exactly.
        sum(case when dca.num_card_issue = 0 then 1 else 0 end) as cards_reissued_0_count,
        sum(case when dca.num_card_issue = 1 then 1 else 0 end) as cards_reissued_1_count,
        sum(case when dca.num_card_issue >= 2 then 1 else 0 end) as cards_reissued_2plus_count,
        sum(coalesce(dca.num_card_issue, 0)) as total_card_reissue_count
    from output_dates as od
    inner join {{ ref('dim_cards') }} as dca
        on dca.effective_from_date < cast(date_add(od.full_date, 1) as timestamp)
       and dca.effective_to_date >= cast(date_add(od.full_date, 1) as timestamp)
    where dca.card_key not in ('-1', '-2')
    group by
        od.date_key,
        case
            when dca.has_chip = true then 'CHIP'
            when dca.has_chip = false then 'NON_CHIP'
            else 'UNKNOWN'
        end,
        coalesce(dca.card_brand, 'UNKNOWN')

),

-- Full outer join, not left: a segment can have transactions with no issued
-- cards (the sentinel bucket) and issued cards with no transactions in the
-- window (section 3). A combination empty on both sides emits no row at all.
combined as (

    select
        coalesce(i.date_key, a.date_key) as date_key,
        to_date(cast(coalesce(i.date_key, a.date_key) as string), 'yyyyMMdd') as full_date,
        coalesce(i.chip_segment, a.chip_segment) as chip_segment,
        coalesce(i.card_brand, a.card_brand) as card_brand,
        coalesce(i.issued_card_count, 0) as issued_card_count,
        coalesce(a.active_card_count_90d, 0) as active_card_count_90d,
        coalesce(a.transaction_count_90d, 0) as transaction_count_90d,
        coalesce(a.successful_transaction_count_90d, 0) as successful_transaction_count_90d,
        coalesce(a.failed_transaction_count_90d, 0) as failed_transaction_count_90d,
        coalesce(i.cards_reissued_0_count, 0) as cards_reissued_0_count,
        coalesce(i.cards_reissued_1_count, 0) as cards_reissued_1_count,
        coalesce(i.cards_reissued_2plus_count, 0) as cards_reissued_2plus_count,
        coalesce(i.total_card_reissue_count, 0) as total_card_reissue_count
    from issued_side as i
    full outer join active_side as a
        on i.date_key = a.date_key
       and i.chip_segment = a.chip_segment
       and i.card_brand = a.card_brand

)

select
    c.date_key,
    c.chip_segment,
    c.card_brand,
    cast(c.issued_card_count as int) as issued_card_count,
    cast(c.active_card_count_90d as int) as active_card_count_90d,
    cast(c.transaction_count_90d as int) as transaction_count_90d,
    cast(c.successful_transaction_count_90d as int) as successful_transaction_count_90d,
    cast(c.failed_transaction_count_90d as int) as failed_transaction_count_90d,
    cast(c.cards_reissued_0_count as int) as cards_reissued_0_count,
    cast(c.cards_reissued_1_count as int) as cards_reissued_1_count,
    cast(c.cards_reissued_2plus_count as int) as cards_reissued_2plus_count,
    cast(c.total_card_reissue_count as int) as total_card_reissue_count,
    -- 90 everywhere except the first 89 days of history, where the window is
    -- truncated (section 5.1). Rows below 90 must not be compared against
    -- full-window rows. Deliberately NOT clamped: backfilling a batch day that
    -- precedes the first transaction is outside this model's contract, and a
    -- value that trips the 1..90 range test is a louder failure than a silently
    -- clamped one.
    cast(
        datediff(
            c.full_date,
            greatest(date_sub(c.full_date, 89), sw.history_start_date)
        ) + 1 as int
    ) as window_day_count
from combined as c
-- spine_window is one row.
cross join spine_window as sw
