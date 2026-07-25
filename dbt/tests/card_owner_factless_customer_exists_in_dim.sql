-- spec docs/helpers/card_owner_factless.md section 9: "customer_key tồn tại
-- trong dim_customers" — every customer_key in the bridge must match a
-- gold.dim_customers row with is_current = true. Written as a singular test
-- rather than a generic relationships test because the parent side has to be
-- filtered to is_current, which relationships cannot express.
--
-- Within a single `dbt build` of this model the check is close to a tautology
-- (the bridge is selected straight out of the same filtered dimension). What it
-- actually guards is staleness ACROSS runs: this project builds models via
-- separately selected commands (scripts/deploy_gold_dbt_dev.sh), so dim_customers
-- can gain a new SCD2 version — closing the version this bridge was built from —
-- without card_owner_factless being rebuilt. Fails if it returns any rows.
select
    b.customer_key
from {{ ref('card_owner_factless') }} as b
left join {{ ref('dim_customers') }} as dc
    on b.customer_key = dc.customer_key
   and dc.is_current
where dc.customer_key is null
