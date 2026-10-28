{#
    One row per raw record, parsed from the JSON payload. Rows are never
    dropped: problems are recorded in validation_errors (row unusable) and
    quality_warnings (row usable, values suspicious).

    The columns shared with staging.product_records follow src/transform.py
    exactly, so both pipelines can be reconciled before the Python transform
    is removed. Columns after "Added in dbt" exist only here.
#}

{% set nutrients = [
    ('energy-kcal', 'kcal', 'energy_kcal'),
    ('energy-kj', 'kJ', 'energy_kj'),
    ('fat', 'g', 'fat_g'),
    ('proteins', 'g', 'proteins_g'),
    ('carbohydrates', 'g', 'carbohydrates_g'),
    ('sugars', 'g', 'sugars_g'),
    ('salt', 'g', 'salt_g'),
    ('saturated-fat', 'g', 'saturated_fat_g'),
] %}

with source as (

    select * from {{ source('openfoodfacts', 'product_records') }}

),

{#
    Payloads are large and TOAST-compressed, and PostgreSQL decompresses the
    whole payload on every "payload -> key". Pull every key needed in a single
    jsonb_to_record call and materialize the result, so each payload is
    decompressed once instead of once per expression.
#}
fields as materialized (

    select
        source.source_file_name,
        source.source_line_number,
        source.loaded_at,
        payload.*
    from source
    cross join lateral jsonb_to_record(
        case when jsonb_typeof(source.payload) = 'object' then source.payload else '{}'::jsonb end
    ) as payload (
        code jsonb,
        created_t jsonb,
        last_modified_t jsonb,
        rev jsonb,
        product_name jsonb,
        brands_tags jsonb,
        categories_tags jsonb,
        countries_tags jsonb,
        nutriscore_grade jsonb,
        nova_group jsonb,
        nutrition jsonb,
        lang jsonb,
        product_type jsonb,
        brands jsonb,
        brand_owner jsonb,
        labels_tags jsonb,
        stores_tags jsonb,
        pnns_groups_1 jsonb,
        pnns_groups_2 jsonb,
        product_quantity jsonb,
        product_quantity_unit jsonb,
        nutriscore_version jsonb,
        nutriscore jsonb,
        completeness jsonb,
        data_quality_errors_tags jsonb
    )

),

nutrition as (

    select
        *,
        case
            when jsonb_typeof(nutrition -> 'aggregated_set') = 'object'
            then nutrition -> 'aggregated_set'
        end as aggregated_set
    from fields

),

nutrition_basis as (

    select
        *,
        aggregated_set -> 'nutrients' as nutrients,
        case
            when jsonb_typeof(aggregated_set -> 'per') = 'string'
                and aggregated_set ->> 'per' in ('100g', '100ml')
            then aggregated_set ->> 'per'
        end as nutrition_basis
    from nutrition

),

parsed as (

    select
        source_file_name,
        source_line_number,
        loaded_at,

        -- product code
        case
            when jsonb_typeof(code) = 'string'
            then {{ json_text("code") }}
        end as product_code,
        case
            when jsonb_typeof(code) is null
                or jsonb_typeof(code) = 'null'
            then 'missing_product_code'
            when jsonb_typeof(code) <> 'string'
            then 'non_digit_product_code'
            when {{ json_text("code") }} is null
            then 'empty_product_code'
            when {{ json_text("code") }} !~ '^[0-9]+$'
            then 'non_digit_product_code'
        end as product_code_error,

        -- timestamps and version
        {{ json_epoch_timestamp("created_t") }} as created_at,
        {{ json_epoch_timestamp("last_modified_t") }} as last_modified_at,
        {{ json_integer("rev") }} as rev,

        -- descriptive attributes
        {{ json_text("product_name") }} as product_name,
        {{ json_text_array("brands_tags") }} as brands_tags,
        {{ json_text_array("categories_tags") }} as categories_tags,
        {{ json_text_array("countries_tags") }} as countries_tags,

        -- Nutri-Score
        case
            when jsonb_typeof(nutriscore_grade) is null
                or jsonb_typeof(nutriscore_grade) = 'null'
            then 'missing'
            when jsonb_typeof(nutriscore_grade) <> 'string'
            then 'unexpected'
            when (nutriscore_grade #>> '{}') in ('a', 'b', 'c', 'd', 'e')
            then 'available'
            when (nutriscore_grade #>> '{}') = 'unknown'
            then 'unknown'
            when (nutriscore_grade #>> '{}') = 'not-applicable'
            then 'not_applicable'
            else 'unexpected'
        end as nutriscore_status,
        case
            when jsonb_typeof(nutriscore_grade) = 'string'
                and (nutriscore_grade #>> '{}') in ('a', 'b', 'c', 'd', 'e')
            then (nutriscore_grade #>> '{}')
        end as nutriscore_grade,
        {{ json_integer("nova_group", 'integer') }} as nova_group,

        -- nutrition per 100 g or 100 ml; values only when the basis is known
        nutrition_basis,
        case
            when aggregated_set is null or nutrition_basis is not null
            then null
            when jsonb_typeof(aggregated_set -> 'per') is null
                or jsonb_typeof(aggregated_set -> 'per') = 'null'
            then
                case
                    when jsonb_typeof(nutrients) = 'object' and nutrients <> '{}'::jsonb
                    then 'missing_nutrition_basis'
                end
            else 'unsupported_nutrition_basis'
        end as nutrition_basis_warning,
        {% for name, unit, column in nutrients %}
        case
            when nutrition_basis is not null
            then {{ nutrient_value('nutrients', name, unit) }}
        end as {{ column }},
        {% endfor %}
        nutrition_basis is not null and (
            {% for name, unit, column in nutrients %}
            (
                jsonb_typeof(nutrients -> '{{ name }}' -> 'value') = 'number'
                and nutrients -> '{{ name }}' ->> 'unit' is distinct from '{{ unit }}'
            ){% if not loop.last %} or{% endif %}
            {% endfor %}
        ) as has_unexpected_nutrition_unit,

        -- Added in dbt
        {{ json_text("lang") }} as lang,
        {{ json_text("product_type") }} as product_type,
        {{ json_text("brands") }} as brands,
        {{ json_text("brand_owner") }} as brand_owner,
        {{ json_text_array("labels_tags") }} as labels_tags,
        {{ json_text_array("stores_tags") }} as stores_tags,
        nullif({{ json_text("pnns_groups_1") }}, 'unknown') as pnns_groups_1,
        nullif({{ json_text("pnns_groups_2") }}, 'unknown') as pnns_groups_2,
        case
            when jsonb_typeof(product_quantity) = 'number'
            then {{ json_numeric("product_quantity") }}
            when {{ json_text("product_quantity") }} ~ '^[0-9]+(\.[0-9]+)?$'
            then ({{ json_text("product_quantity") }})::numeric
        end as product_quantity,
        {{ json_text("product_quantity_unit") }} as product_quantity_unit,
        {{ json_text("nutriscore_version") }} as nutriscore_version,
        case
            when nutriscore -> '2023' ->> 'grade' in ('a', 'b', 'c', 'd', 'e')
            then nutriscore -> '2023' ->> 'grade'
        end as nutriscore_2023_grade,
        {{ json_integer("nutriscore -> '2023' -> 'score'", 'integer') }} as nutriscore_2023_score,
        case
            when nutrition_basis is not null
            then {{ json_text("aggregated_set -> 'preparation'") }}
        end as nutrition_preparation,
        case
            when nutrition_basis is not null
            then {{ nutrient_value('nutrients', 'fiber', 'g') }}
        end as fiber_g,
        case
            when nutrition_basis is not null
            then {{ nutrient_value('nutrients', 'added-sugars', 'g') }}
        end as added_sugars_g,
        -- Open Food Facts estimates some values from the ingredients list.
        -- That is rare for the eight label nutrients but common for fibre and
        -- added sugars (about 75% of added-sugar values), so those are
        -- flagged on their own.
        coalesce(nutrition_basis is not null and (
            {% for name, unit, column in nutrients %}
            nutrients -> '{{ name }}' ->> 'source' = 'estimate'{% if not loop.last %} or{% endif %}
            {% endfor %}
        ), false) as has_estimated_nutrients,
        nutrients -> 'fiber' ->> 'source' as fiber_source,
        nutrients -> 'added-sugars' ->> 'source' as added_sugars_source,
        {{ json_numeric("completeness") }} as completeness,
        {{ json_text_array("data_quality_errors_tags") }} as data_quality_errors_tags

    from nutrition_basis

),

final as (

    select
        source_file_name,
        source_line_number,
        loaded_at,
        product_code,
        created_at,
        last_modified_at,
        rev,
        product_name,
        brands_tags,
        categories_tags,
        countries_tags,
        nutriscore_grade,
        nutriscore_status,
        nova_group,
        nutrition_basis,
        {% for name, unit, column in nutrients %}
        {{ column }},
        {% endfor %}

        array_remove(array[
            product_code_error,
            case when last_modified_at is null then 'invalid_last_modified_t' end
        ], null) as validation_errors,

        -- Same order as src/transform.py. Thresholds: labels round to whole
        -- grams (1 g tolerance) and may count fibre inside carbohydrates
        -- (110 g sum); pure fat is about 900 kcal per 100 g.
        array_remove(array[
            nutrition_basis_warning,
            case when has_unexpected_nutrition_unit then 'unexpected_nutrition_unit' end,
            case when sugars_g > carbohydrates_g + 1 then 'sugars_exceed_carbohydrates' end,
            case when saturated_fat_g > fat_g + 1 then 'saturated_fat_exceeds_fat' end,
            case
                when nutrition_basis = '100g'
                    and greatest(fat_g, proteins_g, carbohydrates_g, sugars_g, salt_g, saturated_fat_g) > 100
                then 'nutrient_exceeds_100g'
            end,
            case
                when nutrition_basis = '100g'
                    and coalesce(fat_g, 0) + coalesce(proteins_g, 0)
                        + coalesce(carbohydrates_g, 0) + coalesce(salt_g, 0) > 110
                then 'heuristic_macronutrient_sum_over_110g'
            end,
            case
                when nutrition_basis = '100g' and energy_kcal > 950
                then 'heuristic_energy_over_950kcal'
            end,
            case when nutriscore_status = 'unexpected' then 'unexpected_nutriscore_value' end
        ], null) as quality_warnings,

        product_code_error is null and last_modified_at is not null as is_valid,

        -- Added in dbt
        lang,
        product_type,
        brands,
        brand_owner,
        labels_tags,
        stores_tags,
        pnns_groups_1,
        pnns_groups_2,
        product_quantity,
        product_quantity_unit,
        nutriscore_version,
        nutriscore_2023_grade,
        nutriscore_2023_score,
        nutrition_preparation,
        fiber_g,
        added_sugars_g,
        has_estimated_nutrients,
        coalesce(fiber_g is not null and fiber_source = 'estimate', false) as fiber_is_estimated,
        coalesce(added_sugars_g is not null and added_sugars_source = 'estimate', false) as added_sugars_is_estimated,
        completeness,
        data_quality_errors_tags

    from parsed

)

select * from final
