-- spec docs/helpers/transaction_errors_bridge.md section 9: "Error bridge ↔
-- fact consistency" — every transaction_id in the bridge must exist in
-- fact_transactions with is_error = true. Fails if it returns any rows.
select
    b.transaction_id
from {{ ref('trans_error_bridge') }} as b
left join {{ ref('fact_transactions') }} as f
    on b.transaction_id = f.transaction_id
   and f.is_error = true
where f.transaction_id is null
