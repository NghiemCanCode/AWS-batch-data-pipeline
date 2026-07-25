-- Full refresh every run (spec docs/helpers/card_owner_factless.md section 2,
-- Decision Log 2026-07-23): no config block needed — marts already default to
-- +materialized: table + +file_format: iceberg in dbt_project.yml, and `table`
-- on dbt-spark is a create-or-replace, which is exactly the truncate + reload
-- the spec calls for. No watermark, no merge keys, no surrogate key.

with current_cards as (

    select
        card_key,
        customer_id
    from {{ ref('dim_cards') }}
    where is_current
      -- spec section 6.2: this table has no default member. dim_cards seeds
      -- card_key '-1' (customer_id 'UNKNOWN') and '-2' ('NOT APPLICABLE'), and
      -- dim_customers seeds the matching customer_id values, so leaving them in
      -- would inner-join into the phantom pairs (-1,-1) and (-2,-2) — an
      -- "Unknown customer owns the Unknown card" row that means nothing.
      and card_key not in ('-1', '-2')

),

current_customers as (

    select
        customer_key,
        customer_id
    from {{ ref('dim_customers') }}
    where is_current
      and customer_key not in ('-1', '-2')

)

-- Inner join, not left join + Unknown member (spec section 5.1, Decision Log
-- #7): dim_cards.customer_id is nullable pass-through (stg_cards.sql), and a
-- card with no owner has no ownership relationship to bridge, so it is dropped
-- rather than mapped to '-1'.
--
-- Both sides resolved at is_current = true only (spec section 3, Decision Log
-- #2). NOTE: that decision's stated rationale — "fact_transactions also
-- resolves customer_key/card_key by is_current" — is stale: fact_transactions
-- was restated to as-of range joins on effective_from/to_date (transactions_fact
-- v.0.0.3, 2026-07-23), so this table is now the only current-only resolver in
-- gold. The decision itself still holds (no consumer needs a point-in-time
-- ownership join yet), only its justification changed.
select
    cust.customer_key,
    card.card_key
from current_cards as card
inner join current_customers as cust
    on card.customer_id = cust.customer_id
