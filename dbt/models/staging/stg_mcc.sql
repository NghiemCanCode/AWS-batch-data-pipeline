with source as (

    select
        trim(mcc_code) as mcc,
        trim(merchant_name) as merchant_category_name
    from {{ source('finance_silver', 'silver_mcc') }}

)

select
    mcc,
    -- TODO(observability): spec section 5.1 calls for "skip record + log warning" when
    -- merchant_category_name is missing. Defaulting to UNKNOWN is implemented below; there's
    -- no logging/alerting hook in this dbt project yet, so the warning half is not implemented.
    coalesce(nullif(merchant_category_name, ''), 'UNKNOWN') as merchant_category_name
from source
