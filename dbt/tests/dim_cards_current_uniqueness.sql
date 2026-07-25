-- spec docs/dimensions/cards_dimension.md section 9: "Current card uniqueness"
-- (Critical) — each card_id must have exactly one record with
-- is_current = true. Closes the dimension spec's former Open Question #6
-- (resolved 2026-07-24). Fails if it returns any rows.
select
    card_id,
    sum(case when is_current then 1 else 0 end) as current_record_count
from {{ ref('dim_cards') }}
group by card_id
having sum(case when is_current then 1 else 0 end) != 1
