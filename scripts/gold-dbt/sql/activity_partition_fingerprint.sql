-- STEP 6b: partition fingerprint, printed before / between / after the two
-- incremental runs. All three printouts must be identical.
select
    count(*)                                                                 as total_rows,
    sum(case when date_key = 20191031 then 1 else 0 end)                     as partition_rows,
    sum(case when date_key = 20191031 and is_active_90d then 1 else 0 end)   as partition_active_customers,
    sum(case when date_key = 20191031 then active_card_count_90d else 0 end) as partition_active_cards
from {{ ref('fact_customer_activity_daily') }}
