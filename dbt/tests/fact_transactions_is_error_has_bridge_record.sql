-- spec docs/facts/transactions_fact.md section 9: "Error bridge consistency" —
-- every fact_transactions row with is_error = true must have >=1 matching
-- record in gold.trans_error_bridge. Fails if it returns any rows.
select
    f.transaction_id
from {{ ref('fact_transactions') }} as f
left join {{ ref('trans_error_bridge') }} as b
    on f.transaction_id = b.transaction_id
where f.is_error = true
  and b.transaction_id is null
