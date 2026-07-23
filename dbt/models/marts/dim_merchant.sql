{{
    config(
        materialized='table'
    )
}}

with deduped as (

    select distinct
        mcc,
        merchant_category_name
    from {{ ref('stg_mcc') }}

),

-- spec section 6.2: mandatory Unknown / Not Applicable member rows so fact_transaction
-- keeps referential integrity when an mcc FK doesn't resolve.
unknown_members as (

    select '-1' as mcc, 'UNKNOWN' as merchant_category_name
    union all
    select '-2' as mcc, 'NOT APPLICABLE' as merchant_category_name

)

select * from deduped
union all
select * from unknown_members
