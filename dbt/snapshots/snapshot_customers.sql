{% snapshot snapshot_customers %}

{#- TODO: move target_schema to a dedicated gold_snapshots Glue database once provisioned —
   snapshots are permanent history, unlike the ephemeral staging tables also living in
   gold_staging. -#}
{#- Raw financial fields (yearly_income, total_debt, credit_score, num_credit_cards) are
   passed through but not tracked here, same as retirement_age/gender/birth_year — only
   the derived income_bracket bucket (plus city/state) triggers a new SCD2 version. These
   raw fields fluctuate too often/noisily to be worth a version each; income_bracket exists
   specifically to give a stable, bucketed attribute for SCD2 tracking. See spec section 6.4. -#}
{#- Tracks raw city/state (not the resolved address_key) since address_key is now
   resolved via a join to gold.dim_geo in dim_customers.sql rather than computed here. -#}
{{
    config(
        target_schema='gold_staging',
        file_format='iceberg',
        unique_key='customer_id',
        strategy='check',
        check_cols=['income_bracket', 'city', 'state'],
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}

-- TODO: add snapshot_customers.yml with docs + tests (unique customer_id + dbt_valid_from,
-- not_null on income_bracket/address_key, etc.) once the full Audit-1 staging test suite
-- lands — out of scope for this MVP pass.
