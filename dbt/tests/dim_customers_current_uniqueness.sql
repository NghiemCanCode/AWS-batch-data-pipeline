-- spec docs/dimensions/customers_dimension.md section 9: "Current customer
-- uniqueness" (Critical) — each customer_id must have exactly one record with
-- is_current = true. Closes the dimension spec's former Open Question #6
-- (resolved 2026-07-24). Fails if it returns any rows.
select
    customer_id,
    sum(case when is_current then 1 else 0 end) as current_record_count
from {{ ref('dim_customers') }}
group by customer_id
having sum(case when is_current then 1 else 0 end) != 1
