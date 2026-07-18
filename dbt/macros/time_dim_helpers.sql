{% macro time_bucket(time_key_col, bucket_size) %}
    cast(
        floor({{ time_key_col }} / 10000) * 100
        + floor(floor(mod({{ time_key_col }}, 10000) / 100) / {{ bucket_size }}) * {{ bucket_size }}
    as smallint)
{% endmacro %}

{% macro display_bucket_time(bucket_col) %}
    concat(
        lpad(cast(floor({{ bucket_col }} / 100) as int), 2, '0'), ':',
        lpad(cast(mod({{ bucket_col }}, 100) as int), 2, '0')
    )
{% endmacro %}
