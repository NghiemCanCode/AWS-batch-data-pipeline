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

-- card_key hashes the snapshot's ORIGINAL dbt_valid_from, not the backdated
-- effective_from_date — keeps the key deterministic and independent of the
-- backdate rule (spec section 6.1, Decision Log v.0.0.3).
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
    -- Version 1 of each card_id is backdated to 1900-01-01 so fact_transactions'
    -- as-of range join covers transactions predating the first snapshot run
    -- (spec section 8.1 step 3, Decision Log v.0.0.3). Later versions keep
    -- dbt_valid_from. versioned computes row_number over the full snapshot
    -- before the incremental filter, so a closed version-1 row re-emitted via
    -- the OR branch below is still recognized as version 1 and re-backdated.
    case
        when version_number = 1 then cast('1900-01-01' as timestamp)
        else dbt_valid_from
    end as effective_from_date,
    coalesce(dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp)) as effective_to_date,
    dbt_valid_to is null as is_current,
    cast(version_number as smallint) as version_number
from versioned

{% if is_incremental() %}
where dbt_valid_from > (select coalesce(max(effective_from_date), timestamp'1900-01-01') from {{ this }})
   or coalesce(dbt_valid_to, timestamp'9999-12-31 23:59:59')
        > (select coalesce(max(effective_from_date), timestamp'1900-01-01') from {{ this }})
{% endif %}

{% if not is_incremental() %}
-- spec section 6.2: mandatory Unknown / Not Applicable member rows so fact_transactions
-- keeps referential integrity when card_key doesn't resolve. customer_id matches the
-- Unknown/Not Applicable customer_id seeded in dim_customers.sql so the customer_id ->
-- dim_customers.customer_id relationship test resolves without exclusions. Only inserted
-- on the initial build / full-refresh; incremental merge preserves them on later runs.
union all

select
    '-1' as card_key,
    'UNKNOWN' as card_id,
    'UNKNOWN' as customer_id,
    'UNKNOWN' as card_brand,
    cast(null as string) as mask_card_number,
    cast(null as smallint) as expires_month,
    cast(null as smallint) as expires_year,
    cast(null as boolean) as has_a_cvv,
    cast(null as boolean) as has_chip,
    cast(null as smallint) as num_card_issue,
    cast('1900-01-01' as timestamp) as effective_from_date,
    cast('9999-12-31 23:59:59' as timestamp) as effective_to_date,
    true as is_current,
    cast(1 as smallint) as version_number

union all

select
    '-2' as card_key,
    'NOT APPLICABLE' as card_id,
    'NOT APPLICABLE' as customer_id,
    'UNKNOWN' as card_brand,
    cast(null as string) as mask_card_number,
    cast(null as smallint) as expires_month,
    cast(null as smallint) as expires_year,
    cast(null as boolean) as has_a_cvv,
    cast(null as boolean) as has_chip,
    cast(null as smallint) as num_card_issue,
    cast('1900-01-01' as timestamp) as effective_from_date,
    cast('9999-12-31 23:59:59' as timestamp) as effective_to_date,
    true as is_current,
    cast(1 as smallint) as version_number
{% endif %}
