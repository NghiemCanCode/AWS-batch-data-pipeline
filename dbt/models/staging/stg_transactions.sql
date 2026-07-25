with source as (

    select
        transaction_id,
        timestamp,
        client_id,
        card_id,
        amount,
        transaction_channel,
        merchant_id,
        trim(merchant_city) as merchant_city,
        trim(merchant_state) as merchant_state,
        coalesce(nullif(trim(zip), ''), 'UNKNOWN') as zip,
        trim(cast(mcc as string)) as mcc,
        is_error,
        errors
    from {{ source('finance_silver', 'silver_transactions') }}
    where transaction_id is not null

)

select
    transaction_id,
    timestamp,
    client_id,
    card_id,
    amount,
    -- spec (docs/facts/transactions_fact.md section 5.1): Degenerate Dimension,
    -- normalized to the accepted value set, else UNKNOWN. Mirrors the
    -- card_brand normalization pattern in stg_cards.sql.
    case
        when upper(trim(transaction_channel)) in
            ('SWIPE TRANSACTION', 'ONLINE TRANSACTION', 'CHIP TRANSACTION')
            then upper(trim(transaction_channel))
        else 'UNKNOWN'
    end as transaction_type,
    merchant_id,
    -- merchant_city/merchant_state/zip are trimmed the same way stg_geo.sql
    -- trims them, so fact_transactions' merchant_geo_key join matches dim_geo
    -- rows exactly.
    merchant_city,
    merchant_state,
    zip,
    mcc,
    is_error,
    errors
from source
