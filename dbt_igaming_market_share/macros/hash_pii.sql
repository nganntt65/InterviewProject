{% macro hash_pii(column_name) %}
    sha2(coalesce(cast({{ column_name }} as string), ''), 256)
{% endmacro %}