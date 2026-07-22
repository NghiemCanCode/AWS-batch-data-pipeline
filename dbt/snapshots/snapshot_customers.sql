{% snapshot snapshot_customers %}

{#- TODO: move target_schema to a dedicated gold_snapshots Glue database once provisioned —
   snapshots are permanent history, unlike the ephemeral staging tables also living in
   gold_staging. -#}
{#- Raw financial fields (yearly_income, total_debt, credit_score, num_credit_cards) are
   passed through but not tracked here, same as retirement_age/gender/birth_year — only
   the derived income_bracket bucket triggers a new SCD2 version. Spec section 6.4 leaves
   this undecided; revisit if point-in-time raw-value history is ever required. -#}
{{
    config(
        target_schema='gold_staging',
        file_format='iceberg',
        unique_key='customer_id',
        strategy='check',
        check_cols=['income_bracket', 'address_key'],
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}

-- TODO: add snapshot_customers.yml with docs + tests (unique customer_id + dbt_valid_from,
-- not_null on income_bracket/address_key, etc.) once the full Audit-1 staging test suite
-- lands — out of scope for this MVP pass.
