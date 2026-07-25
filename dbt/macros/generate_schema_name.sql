{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use the custom schema name (e.g. `gold_staging`) as-is instead of
       dbt's default `<target_schema>_<custom_schema>` prefixing, so
       +schema config maps 1:1 onto the pre-provisioned Glue databases. -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
