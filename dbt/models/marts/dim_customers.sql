{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_key',
    )
}}

with versioned as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by dbt_valid_from
        ) as version_number
    from {{ ref('snapshot_customers') }}

)

-- address_key resolves via a real join to gold.dim_geo (matching zip = 'UNKNOWN', since
-- silver.users never carries a real zip) instead of self-hashing city/state. This makes
-- dim_customers depend on dim_geo having already been built.
-- customer_key hashes the snapshot's ORIGINAL dbt_valid_from, not the backdated
-- effective_from_date — keeps the key deterministic and independent of the
-- backdate rule (spec section 6.1, Decision Log v.0.0.3).
select
    md5(concat_ws('||', versioned.customer_id, cast(versioned.dbt_valid_from as string))) as customer_key,
    versioned.customer_id,
    versioned.retirement_age,
    versioned.gender,
    versioned.birth_year,
    coalesce(dg.location_key, '-1') as address_key,
    versioned.yearly_income,
    versioned.income_bracket,
    versioned.total_debt,
    versioned.credit_score,
    versioned.num_credit_cards,
    -- Version 1 of each customer_id is backdated to 1900-01-01 so fact_transactions'
    -- as-of range join covers transactions predating the first snapshot run
    -- (spec section 8.1 step 3, Decision Log v.0.0.3). Later versions keep
    -- dbt_valid_from. versioned computes row_number over the full snapshot
    -- before the incremental filter, so a closed version-1 row re-emitted via
    -- the OR branch below is still recognized as version 1 and re-backdated.
    case
        when versioned.version_number = 1 then cast('1900-01-01' as timestamp)
        else versioned.dbt_valid_from
    end as effective_from_date,
    coalesce(versioned.dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp)) as effective_to_date,
    versioned.dbt_valid_to is null as is_current,
    cast(versioned.version_number as smallint) as version_number
from versioned
left join {{ ref('dim_geo') }} as dg
    on nullif(versioned.city, '') = dg.city
   and nullif(versioned.state, '') = dg.state
   and dg.zip = 'UNKNOWN'

{% if is_incremental() %}
where versioned.dbt_valid_from > (select coalesce(max(effective_from_date), timestamp'1900-01-01') from {{ this }})
   or coalesce(versioned.dbt_valid_to, timestamp'9999-12-31 23:59:59')
        > (select coalesce(max(effective_from_date), timestamp'1900-01-01') from {{ this }})
{% endif %}

{% if not is_incremental() %}
-- spec section 6.2: mandatory Unknown / Not Applicable member rows so fact_transactions
-- keeps referential integrity when customer_key doesn't resolve. Only inserted on the
-- initial build / full-refresh; incremental merge preserves them on later runs.
union all

select
    '-1' as customer_key,
    'UNKNOWN' as customer_id,
    cast(null as smallint) as retirement_age,
    'UNKNOWN' as gender,
    cast(null as int) as birth_year,
    '-1' as address_key,
    cast(null as decimal(18,2)) as yearly_income,
    'UNKNOWN' as income_bracket,
    cast(null as decimal(18,2)) as total_debt,
    cast(null as int) as credit_score,
    cast(null as int) as num_credit_cards,
    cast('1900-01-01' as timestamp) as effective_from_date,
    cast('9999-12-31 23:59:59' as timestamp) as effective_to_date,
    true as is_current,
    cast(1 as smallint) as version_number

union all

select
    '-2' as customer_key,
    'NOT APPLICABLE' as customer_id,
    cast(null as smallint) as retirement_age,
    'UNKNOWN' as gender,
    cast(null as int) as birth_year,
    '-2' as address_key,
    cast(null as decimal(18,2)) as yearly_income,
    'UNKNOWN' as income_bracket,
    cast(null as decimal(18,2)) as total_debt,
    cast(null as int) as credit_score,
    cast(null as int) as num_credit_cards,
    cast('1900-01-01' as timestamp) as effective_from_date,
    cast('9999-12-31 23:59:59' as timestamp) as effective_to_date,
    true as is_current,
    cast(1 as smallint) as version_number
{% endif %}
