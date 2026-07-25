with transactions_geo as (

    select
        trim(merchant_city) as city,
        trim(merchant_state) as state,
        coalesce(nullif(trim(zip), ''), 'UNKNOWN') as zip
    from {{ source('finance_silver', 'silver_transactions') }}

),

users_geo as (

    -- silver.users chưa enrich field zip (spec section 5.1) -> dùng giá trị Default.
    select
        trim(city) as city,
        trim(state) as state,
        'UNKNOWN' as zip
    from {{ source('finance_silver', 'silver_users') }}

),

unioned as (

    select * from transactions_geo
    union all
    select * from users_geo

)

-- TODO(observability): spec section 5.1 calls for "skip record + log warning" when
-- city/state are null. The skip is implemented below; there's no logging/alerting hook
-- in this dbt project yet, so the warning half is not implemented.
select city, state, zip
from unioned
where nullif(city, '') is not null
  and nullif(state, '') is not null
