-- STEP 6b probe: find the data day to pass as --vars batch_logical_date.
select
    min(date_key) as min_dk,
    max(date_key) as max_dk
from {{ ref('fact_transactions') }}
