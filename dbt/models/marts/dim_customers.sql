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
    versioned.dbt_valid_from as effective_from_date,
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
