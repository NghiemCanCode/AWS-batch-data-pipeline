{#-
  Overrides dbt-spark's built-in spark__create_csv_table to emit
  `create or replace table` instead of a bare `create table`.

  Why: dbt-spark lists existing relations with `show table extended in <schema>
  like '*'`, a Hive-style command that errors out on Iceberg tables under
  SparkSessionCatalog ("StorageDescriptor#InputFormat cannot be null" — Iceberg
  tables legitimately have a null Hive InputFormat, see Glue table_type=ICEBERG).
  The listing dies on the first Iceberg table in the schema, so dbt sees an empty
  relation cache and treats every object as non-existent.

  Models tolerate this because their materializations already emit
  `create or replace table`. Seeds are the exception: dbt's seed materialization
  branches on old_relation, so a permanently-empty cache always routes to
  create_csv_table, which then collides with the real table
  (TABLE_OR_VIEW_ALREADY_EXISTS). Making the create idempotent sidesteps the
  broken cache entirely — the seed here is a small static reference table, so
  rewriting it on every run costs nothing.

  Body is otherwise a verbatim copy of the adapter macro; re-check it against
  dbt-spark on upgrade.
-#}

{% macro spark__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create or replace table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ location_clause() }}
    {{ comment_clause() }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}
