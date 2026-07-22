{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='card_key',
    )
}}

with versioned as (

    select
        *,
        row_number() over (
            partition by card_id
            order by dbt_valid_from
        ) as version_number
    from {{ ref('snapshot_cards') }}

)

-- TODO: spec (docs/dimensions/cards_dimension.md section 6.2) requires seeded
-- Unknown (-1) / Not Applicable (-2) member rows for referential integrity from
-- fact_transaction. Not implemented yet — same gap exists in dim_customers.

select
    md5(concat_ws('||', card_id, cast(dbt_valid_from as string))) as card_key,
    card_id,
    customer_id,
    card_brand,
    mask_card_number,
    expires_month,
    expires_year,
    has_a_cvv,
    has_chip,
    num_card_issue,
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
