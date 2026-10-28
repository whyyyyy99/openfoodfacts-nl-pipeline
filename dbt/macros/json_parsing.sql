{#
    Tolerant JSONB parsing: a value of the wrong JSON type becomes NULL instead
    of failing the whole query, matching the helpers in src/transform.py.
#}

{% macro json_text(expr) -%}
    case
        when jsonb_typeof({{ expr }}) = 'string'
        then nullif(regexp_replace({{ expr }} #>> '{}', '^\s+|\s+$', '', 'g'), '')
    end
{%- endmacro %}


{% macro json_integer(expr, type='bigint') -%}
    {%- set bounds = {
        'bigint': ('-9223372036854775808', '9223372036854775807'),
        'integer': ('-2147483648', '2147483647'),
    }[type] -%}
    {#- Integral JSON numbers only: 5.0 stays NULL, as it is a float in Python. -#}
    case
        when jsonb_typeof({{ expr }}) = 'number'
            and ({{ expr }})::text ~ '^-?[0-9]+$'
            and ({{ expr }})::text::numeric between {{ bounds[0] }} and {{ bounds[1] }}
        then ({{ expr }})::text::{{ type }}
    end
{%- endmacro %}


{% macro json_numeric(expr) -%}
    case when jsonb_typeof({{ expr }}) = 'number' then ({{ expr }})::text::numeric end
{%- endmacro %}


{% macro json_epoch_timestamp(expr) -%}
    {#- Python's datetime only covers years 1 to 9999. -#}
    case
        when {{ json_integer(expr) }} between -62135596800 and 253402300799
        then to_timestamp({{ json_integer(expr) }})
    end
{%- endmacro %}


{% macro json_text_array(expr) -%}
    {#- An array with any non-string element is rejected as a whole. -#}
    case
        when jsonb_typeof({{ expr }}) = 'array'
            and not jsonb_path_exists({{ expr }}, '$[*] ? (@.type() != "string")')
        then array(
            select element
            from jsonb_array_elements_text({{ expr }}) with ordinality as t (element, position)
            order by position
        )
    end
{%- endmacro %}


{% macro nutrient_value(nutrients, name, unit) -%}
    {#- A value in an unexpected unit is dropped; the model flags it separately. -#}
    case
        when jsonb_typeof({{ nutrients }} -> '{{ name }}' -> 'value') = 'number'
            and {{ nutrients }} -> '{{ name }}' ->> 'unit' = '{{ unit }}'
        then ({{ nutrients }} -> '{{ name }}' ->> 'value')::numeric
    end
{%- endmacro %}
