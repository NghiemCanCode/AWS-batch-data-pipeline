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

select
    md5(concat_ws('||', customer_id, cast(dbt_valid_from as string))) as customer_key,
    customer_id,
    retirement_age,
    gender,
    birth_year,
    address_key,
    yearly_income,
    income_bracket,
    total_debt,
    credit_score,
    num_credit_cards,
    dbt_valid_from as effective_from_date,
    coalesce(dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp)) as effective_to_date,
    dbt_valid_to is null as is_current,
    cast(version_number as smallint) as version_number
from versioned

{% if is_incremental() %}
where dbt_valid_from > (select coalesce(max(effective_from_date), timestamp'1900-01-01') from {{ this }})
   or coalesce(dbt_valid_to, timestamp'9999-12-31 23:59:59')
        > (select coalesce(max(effective_from_date), timestamp'1900-01-01') from {{ this }})
{% endif %}
