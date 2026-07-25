-- spec docs/dimensions/cards_dimension.md section 9: "SCD2 interval coverage"
-- (Critical) — per card_id, validity intervals must run contiguously from
-- 1900-01-01 (version 1 backdate) to 9999-12-31 23:59:59 with no gap; a gap
-- makes fact_transactions' as-of join fall through to Unknown ('-1'). The
-- seeded '-1'/'-2' member rows each cover the full range in a single version,
-- so no exclusion is needed. Fails if it returns any rows.
with ordered_versions as (

    select
        card_id,
        effective_from_date,
        effective_to_date,
        lead(effective_from_date) over (
            partition by card_id
            order by effective_from_date
        ) as next_effective_from_date
    from {{ ref('dim_cards') }}

),

-- gap between consecutive versions
gaps as (

    select card_id, 'gap between versions' as violation
    from ordered_versions
    where next_effective_from_date > effective_to_date

),

-- first version must start at 1900-01-01, last must end at 9999-12-31 23:59:59
bounds as (

    select card_id, 'bounds not covered' as violation
    from ordered_versions
    group by card_id
    having min(effective_from_date) != cast('1900-01-01' as timestamp)
        or max(effective_to_date) != cast('9999-12-31 23:59:59' as timestamp)

)

select * from gaps
union all
select * from bounds
