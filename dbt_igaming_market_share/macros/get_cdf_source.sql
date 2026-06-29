{% macro get_cdf_source(source_relation, start_point) %}
    table_changes('{{ source_relation }}', {{ start_point }})
{% endmacro %}