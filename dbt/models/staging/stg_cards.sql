with source as (

    select
        card_id,
        client_id,
        card_brand,
        mask_card_number,
        expires,
        has_a_cvv,
        has_chip,
        num_cards_issued
    from {{ source('finance_silver', 'silver_cards') }}
    where card_id is not null

)

select
    -- TODO(security): spec (docs/dimensions/cards_dimension.md section 10) calls for
    -- card_id and customer_id to be SHA-256 hashed before landing in Gold (DI-level PII).
    -- Not implemented yet, same open item as stg_customers.sql's customer_id.
    card_id,
    -- customer_id kept nullable/pass-through: a card can exist in this dimension without
    -- a resolved owner. Spec v.0.0.2 (sections 5.1/7 + Decision Log) settled the former
    -- 5.1-vs-7 conflict on NULL passthrough — no skip/remap implemented here.
    client_id as customer_id,
    case
        when upper(trim(card_brand)) in ('VISA', 'MASTERCARD', 'AMEX', 'DISCOVER')
            then upper(trim(card_brand))
        else 'UNKNOWN'
    end as card_brand,
    mask_card_number,
    cast(month(expires) as smallint) as expires_month,
    cast(year(expires) as smallint) as expires_year,
    has_a_cvv,
    has_chip,
    cast(num_cards_issued as smallint) as num_card_issue
from source

-- TODO(security): spec section 10 also flags expires_month/expires_year as QI-level PII
-- requiring Lake Formation column-level policies. Not enforceable yet — this project
-- hasn't migrated to Lake Formation.
