{% macro generate_schema_name(custom_schema_name, node) -%}

    {# If no custom schema is defined, fall back to the default target schema #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {# If a custom schema exists (like silver or gold), use it exactly as written #}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}