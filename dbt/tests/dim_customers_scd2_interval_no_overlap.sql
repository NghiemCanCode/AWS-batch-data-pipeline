-- spec docs/dimensions/customers_dimension.md section 9: "SCD2 interval no-overlap"
-- (Critical) — per customer_id, no two versions may have overlapping
-- [effective_from_date, effective_to_date) intervals; an overlap fans out
-- fact_transactions' as-of range join. Fails if it returns any rows.
with ordered_versions as (

    select
        customer_id,
        effective_from_date,
        effective_to_date,
        lead(effective_from_date) over (
            partition by customer_id
            order by effective_from_date
        ) as next_effective_from_date
    from {{ ref('dim_customers') }}

)

select
    customer_id,
    effective_from_date,
    effective_to_date,
    next_effective_from_date
from ordered_versions
where next_effective_from_date < effective_to_date
