{{
    config(
        materialized='table'
    )
}}

with deduped as (

    select distinct
        city,
        state,
        zip
    from {{ ref('stg_geo') }}

),

hashed as (

    select
        sha2(concat_ws('||', city, state, zip), 256) as location_key,
        city,
        state,
        zip
    from deduped

),

-- spec section 6.2: mandatory Unknown / Not Applicable member rows so fact_transaction
-- keeps referential integrity when a location FK doesn't resolve.
unknown_members as (

    select '-1' as location_key, 'UNKNOWN' as city, 'UNKNOWN' as state, 'UNKNOWN' as zip
    union all
    select '-2' as location_key, 'NOT APPLICABLE' as city, 'NOT APPLICABLE' as state, 'NOT APPLICABLE' as zip

)

select * from hashed
union all
select * from unknown_members
