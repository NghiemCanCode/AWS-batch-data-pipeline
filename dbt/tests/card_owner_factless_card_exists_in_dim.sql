-- spec docs/helpers/card_owner_factless.md section 9: "card_key tồn tại trong
-- dim_cards" — every card_key in the bridge must match a gold.dim_cards row
-- with is_current = true. Singular test for the same reason as its
-- customer_key counterpart (see
-- tests/card_owner_factless_customer_exists_in_dim.sql): the parent side needs
-- an is_current filter, and the real failure mode it guards is a dimension
-- rebuilt after the bridge, not a bad join inside one run.
-- Fails if it returns any rows.
select
    b.card_key
from {{ ref('card_owner_factless') }} as b
left join {{ ref('dim_cards') }} as dca
    on b.card_key = dca.card_key
   and dca.is_current
where dca.card_key is null
