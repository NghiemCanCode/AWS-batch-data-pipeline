-- STEP 8: snapshot state, printed either side of a re-run. The two printouts
-- must be identical — see README.md §14.
select * from (
    select
        'snapshot_customers'                                  as snapshot_name,
        count(*)                                              as row_count,
        min(dbt_valid_from)                                   as min_vf,
        max(dbt_valid_from)                                   as max_vf,
        sum(case when dbt_valid_to is null then 1 else 0 end) as open_rows
    from {{ ref('snapshot_customers') }}
    union all
    select
        'snapshot_cards',
        count(*),
        min(dbt_valid_from),
        max(dbt_valid_from),
        sum(case when dbt_valid_to is null then 1 else 0 end)
    from {{ ref('snapshot_cards') }}
) order by snapshot_name
