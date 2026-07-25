{#-
  Overrides dbt-spark's built-in spark__list_relations_without_caching to list
  relations with `show tables` instead of `show table extended`.

  Why: under this project's Spark config (spark.sql.catalogImplementation=hive +
  spark_catalog = SparkSessionCatalog, needed so Hive silver and Iceberg gold
  resolve under one catalog), `show table extended in <schema> like '*'` goes
  through Glue's Hive client, which fetches StorageDescriptor#InputFormat for
  every table. Iceberg tables legitimately have a null InputFormat (Glue marks
  them table_type=ICEBERG), so the command dies on the first Iceberg table in
  the schema:

    org.apache.hadoop.hive.ql.metadata.HiveException: Unable to fetch table
    card_owner_factless. StorageDescriptor#InputFormat cannot be null

  dbt-spark 1.10.3 swallowed that error and returned [] — an empty relation
  cache that silently turned every snapshot merge into a create-or-replace
  (docs/known_issues/dbt_spark_relation_cache.md). dbt-spark 1.11.0 changed that
  branch to `raise` (impl.py list_relations_without_caching, ~line 270), so the
  same defect now aborts the whole invocation before any node runs. Both
  behaviours have the same root cause and the same fix: never issue
  `show table extended` here.

  `show tables in <schema> like '*'` only calls the metastore's listTables (name
  listing), so it never touches StorageDescriptor. It is the exact statement
  dbt-spark's own Iceberg fallback path uses.

  Shape: SparkAdapter._get_relation_information unpacks four values per row
  (schema, name, is_temporary, information), while `show tables` returns three.
  The fourth is rebuilt here.

  Hardcoding 'Provider: iceberg' is NOT decoration — dbt branches on
  relation.is_iceberg in the snapshot materialization (temp-view naming inside
  `merge into`, since Iceberg catalogs reject schema-qualified `create view`)
  and in incremental/strategies.sql. It is safe here only because, as of
  2026-07-24:
    - no model in this project is materialized as a view or ephemeral, so
      nothing can be mislabelled Table when it is a View;
    - gold and gold_staging are entirely Iceberg (dbt_project.yml sets
      +file_format: iceberg on staging, marts and seeds);
    - finance_silver (Hive) is never a target schema, so it never enters the
      cache.
  If either of the first two stops holding, switch to the faithful version:
  loop `describe extended <table>` per relation and read the real Provider,
  the way SparkAdapter._get_relation_information_using_describe does.

  Known gap: `information` no longer carries the per-column schema block that
  `show table extended` emitted, so `dbt docs generate` will report no columns
  for these relations. dbt-spark's own Iceberg fallback has the same gap. It
  affects the catalog artifact only, never a run.
-#}

{% macro spark__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show tables in {{ relation.schema }} like '*'
  {% endcall %}

  {%- set result = load_result('list_relations_without_caching').table -%}
  {%- set relations = [] -%}
  {%- for row in result.rows -%}
    {#- row is (namespace, tableName, isTemporary); append the synthetic
        information column expected by _get_relation_information. Positional
        access on purpose: the first column is named `database` on older Spark
        and `namespace` on 3.x. -#}
    {%- do relations.append((row[0], row[1], row[2], 'Provider: iceberg')) -%}
  {%- endfor -%}

  {% do return(relations) %}
{% endmacro %}
