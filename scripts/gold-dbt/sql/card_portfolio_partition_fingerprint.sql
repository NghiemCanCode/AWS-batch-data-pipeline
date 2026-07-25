-- STEP 6c: partition fingerprint, printed before / between / after the two
-- incremental runs. All three printouts must be identical.
--
-- issued_cards and active_cards are summed ACROSS SEGMENTS within the one
-- partition, which is the only direction those columns are additive — never
-- across date_key (spec section 5.1, registry rules #9 / #10). That makes this
-- a fingerprint, not a metric.
select
    count(*)                                                                      as total_rows,
    sum(case when date_key = 20191031 then 1 else 0 end)                          as partition_rows,
    sum(case when date_key = 20191031 then issued_card_count else 0 end)          as issued_cards,
    sum(case when date_key = 20191031 then active_card_count_90d else 0 end)      as active_cards,
    sum(case when date_key = 20191031 then transaction_count_90d else 0 end)      as window_txns,
    sum(case when date_key = 20191031 then failed_transaction_count_90d else 0 end) as window_failed
from {{ ref('rpt_card_portfolio') }}
