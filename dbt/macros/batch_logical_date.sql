{% macro batch_logical_date() %}
{#-
    spec (docs/facts/user_monthly_snapshot_fact.md section 2, 8.1): "tháng cần
    recompute" mỗi lần chạy = tháng chứa batch_logical_date. Legacy PySpark
    side gets this from a required BATCH_LOGICAL_DATE env var; dbt has no
    equivalent yet, so this introduces `batch_logical_date` as a dbt var,
    defaulting to current_date() when not passed via --vars (no orchestrator
    wires this in yet — dags/aws_pipeline.py is an empty stub). Override with
    `--vars '{batch_logical_date: YYYY-MM-DD}'` to backfill/recompute a
    specific past month.
-#}
{%- if var('batch_logical_date', none) is not none -%}
to_date('{{ var("batch_logical_date") }}')
{%- else -%}
current_date()
{%- endif -%}
{% endmacro %}
