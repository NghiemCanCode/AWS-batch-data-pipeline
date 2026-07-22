with source as (

    select
        user_id,
        retirement_age,
        birth_month,
        birth_year,
        gender,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards,
        trim(city) as city,
        trim(state) as state
    from {{ source('finance_silver', 'silver_users') }}
    where user_id is not null

)

select
    -- TODO(security): spec (docs/dimensions/customers_dimension.md section 10) calls for
    -- customer_id to be SHA-256 hashed before landing in Gold (DI-level PII). Not implemented
    -- yet — pending a decision on how downstream facts would then join on the hashed key.
    user_id as customer_id,
    retirement_age,
    birth_month,
    birth_year,
    case
        when upper(trim(gender)) in ('MALE', 'FEMALE', 'OTHER') then upper(trim(gender))
        else 'UNKNOWN'
    end as gender,
    yearly_income,
    case
        when yearly_income < 60000 then 'Low'
        when yearly_income <= 180000 then 'Middle'
        when yearly_income is not null then 'High'
        else 'Unknown'
    end as income_bracket,
    case
        when city != '' and state != '' then md5(concat(city, state))
        else '-1'
    end as address_key,
    total_debt,
    credit_score,
    num_credit_cards
from source

-- TODO(security): spec section 10 also flags yearly_income, total_debt, credit_score,
-- gender and birth_year as QI-level PII requiring Lake Formation column-level policies.
-- Not enforceable yet — this project hasn't migrated to Lake Formation.
