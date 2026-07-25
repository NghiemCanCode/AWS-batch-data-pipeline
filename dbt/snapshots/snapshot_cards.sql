{% snapshot snapshot_cards %}

{#- TODO: move target_schema to a dedicated gold_snapshots Glue database once provisioned —
   snapshots are permanent history, unlike the ephemeral staging tables also living in
   gold_staging. Same open item as snapshot_customers.sql. -#}
{#- mask_card_number and card_brand are passed through but not tracked here — spec section
   6.4 marks mask_card_number as Type 1 (data correction), and card_brand isn't listed as
   SCD2-tracked either. Only has_chip/has_a_cvv/expires_month/expires_year/num_card_issue
   trigger a new version. -#}
{{
    config(
        target_schema='gold_staging',
        file_format='iceberg',
        unique_key='card_id',
        strategy='check',
        check_cols=['has_chip', 'has_a_cvv', 'expires_month', 'expires_year', 'num_card_issue'],
    )
}}

select * from {{ ref('stg_cards') }}

{% endsnapshot %}

-- TODO: add snapshot_cards.yml with docs + tests (unique card_id + dbt_valid_from,
-- not_null on has_chip/has_a_cvv/etc.) once the full Audit-1 staging test suite lands —
-- out of scope for this MVP pass, same as snapshot_customers.sql.
