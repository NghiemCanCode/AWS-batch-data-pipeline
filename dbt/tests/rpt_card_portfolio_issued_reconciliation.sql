-- spec (docs/metrics/card_portfolio_report.md section 9): "Reconcile issued ->
-- dim_cards" — sum(issued_card_count) across every segment on the batch day
-- must equal the number of dim_cards versions in effect at the END of that day,
-- seed members excluded, recomputed straight from the dimension.
--
-- This check is honest about what it can and cannot catch. It re-applies the
-- same as-of predicate the model uses, so it does NOT independently verify the
-- as-of semantics themselves; what it does verify is that no card was lost or
-- duplicated on the way from the dimension to the output grain — a broken date
-- spine, a full outer join that dropped a segment, or a group by that split one
-- card across two segments. That is the same rationale section 9 gives for the
-- sibling transaction reconciliation.
--
-- Both seed members are excluded ('-1' Unknown, '-2' Not Applicable), matching
-- the model: they are referential-integrity placeholders, not issued cards.
--
-- "End of day D": with [from, to) validity that is the instant just before
-- midnight of D+1, hence the comparison against D+1 and the strict `<` on
-- effective_from_date — a version starting exactly at midnight of D+1 did not
-- exist during day D.
--
-- Scoped to only the batch day (the partition insert_overwrite last touched),
-- not full history — cheaper per run, matches this model's own output scope, so
-- it reuses the same batch_logical_date() macro the model uses to pick that
-- partition.
--
-- coalesce(..., 0) on the report side is deliberate: without it, a missing
-- partition makes sum() return NULL, and `NULL != <n>` is NULL, so the test
-- would silently pass on the one failure mode it most needs to catch (the same
-- fix the fact_daily_transaction_trend reconciliations carry, and that
-- fact_user_monthly_snapshot's still has as a TODO).
--
-- The model_range guard exists because this test cannot fall back to a trivial
-- 0 = 0 the way the sibling reconciliations do. Their source side is a
-- date-filtered scan of fact_transactions, which is empty for a day outside
-- history; ours is dim_cards, whose versions are in effect on EVERY date
-- including today, so a full-refresh run with no --vars (batch_logical_date()
-- falls back to current_date()) would compare 0 against the whole portfolio and
-- fail on a run that did nothing wrong. Restricting the comparison to days
-- inside the range the model actually built restores the trivial pass for that
-- case WITHOUT reintroducing the silent-pass bug: a partition missing from the
-- middle of the built range is still inside min..max and still fails loudly.
-- Pass --vars '{batch_logical_date: <data day>}' to check a day the model
-- actually built.

with batch_day as (

    select
        cast(date_format({{ batch_logical_date() }}, 'yyyyMMdd') as int) as date_key,
        {{ batch_logical_date() }} as batch_date

),

model_range as (

    select
        min(date_key) as min_date_key,
        max(date_key) as max_date_key
    from {{ ref('rpt_card_portfolio') }}

),

report_total as (

    select coalesce(sum(issued_card_count), 0) as issued_card_count
    from {{ ref('rpt_card_portfolio') }}
    where date_key = (select date_key from batch_day)

),

source_total as (

    -- batch_day and model_range are single rows, so neither cross join can fan
    -- out; they just make the scalars available without repeating subqueries.
    select count(*) as issued_card_count
    from {{ ref('dim_cards') }} as dca
    cross join batch_day as bd
    cross join model_range as mr
    where dca.card_key not in ('-1', '-2')
      and dca.effective_from_date < cast(date_add(bd.batch_date, 1) as timestamp)
      and dca.effective_to_date >= cast(date_add(bd.batch_date, 1) as timestamp)
      and bd.date_key between mr.min_date_key and mr.max_date_key

)

select
    report_total.issued_card_count as report_issued_card_count,
    source_total.issued_card_count as source_issued_card_count
from report_total
cross join source_total
where report_total.issued_card_count != source_total.issued_card_count
