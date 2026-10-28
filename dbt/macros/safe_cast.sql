{#
    Unit tests cast fixture values to the types in information_schema, which
    reports every PostgreSQL array as plain ARRAY. All arrays in this project
    are text[], so cast to that instead of failing on "as ARRAY".
#}
{% macro postgres__safe_cast(field, type) -%}
    {%- if type | upper == 'ARRAY' -%}
        cast({{ field }} as text[])
    {%- else -%}
        cast({{ field }} as {{ type }})
    {%- endif -%}
{%- endmacro %}
