-- Reproducible staging-semantics findings for the currently loaded file set.

-- Nutri-Score: distinguish usable grades from source status values.
SELECT
    nutriscore_status,
    nutriscore_grade,
    count(*) AS rows
FROM staging.product_records
GROUP BY nutriscore_status, nutriscore_grade
ORDER BY nutriscore_status, nutriscore_grade;

-- Every emitted warning and the number of affected staging rows.
SELECT
    warning,
    count(*) AS rows
FROM staging.product_records
CROSS JOIN LATERAL unnest(quality_warnings) AS warning
GROUP BY warning
ORDER BY warning;

-- Product-name normalisation and possible two-letter-language fallback coverage.
SELECT
    count(*) FILTER (WHERE s.product_name IS NULL) AS null_product_names,
    count(*) FILTER (
        WHERE s.product_name IS NOT NULL AND btrim(s.product_name) = ''
    ) AS non_null_blank_product_names,
    count(*) FILTER (
        WHERE s.product_name IS NULL
          AND EXISTS (
              SELECT 1
              FROM jsonb_each(r.payload) AS field(key, value)
              WHERE field.key ~ '^product_name_[a-z]{2}$'
                AND jsonb_typeof(field.value) = 'string'
                AND btrim(field.value #>> '{}') <> ''
          )
    ) AS null_names_with_two_letter_fallback
FROM staging.product_records AS s
JOIN raw.product_records AS r
  USING (source_file_name, source_line_number);

-- Denominator for warnings whose mass-bound logic applies only to 100g rows.
SELECT
    count(*) AS rows_with_100g_basis
FROM staging.product_records
WHERE nutrition_basis = '100g';

-- A latest-state winner may be less complete than one of its older versions.
WITH older_versions AS (
    SELECT
        s.product_code,
        bool_or(s.nutriscore_grade IS NOT NULL) AS had_nutriscore_grade,
        bool_or(s.product_name IS NOT NULL) AS had_product_name,
        bool_or(s.nutrition_basis IS NOT NULL) AS had_nutrition_basis,
        bool_or(s.nova_group IS NOT NULL) AS had_nova_group
    FROM staging.product_records AS s
    JOIN mart.products AS m USING (product_code)
    WHERE s.is_valid
      AND (s.source_file_name, s.source_line_number)
          <> (m.source_file_name, m.source_line_number)
    GROUP BY s.product_code
)
SELECT
    count(*) FILTER (
        WHERE m.nutriscore_grade IS NULL AND o.had_nutriscore_grade
    ) AS latest_lost_nutriscore_grade,
    count(*) FILTER (
        WHERE m.product_name IS NULL AND o.had_product_name
    ) AS latest_lost_product_name,
    count(*) FILTER (
        WHERE m.nutrition_basis IS NULL AND o.had_nutrition_basis
    ) AS latest_lost_nutrition_basis,
    count(*) FILTER (
        WHERE m.nova_group IS NULL AND o.had_nova_group
    ) AS latest_lost_nova_group
FROM mart.products AS m
JOIN older_versions AS o USING (product_code);

-- Source values that vary across otherwise valid versions.
SELECT
    count(*) FILTER (WHERE distinct_created_at > 1)
        AS product_codes_with_changed_created_at,
    count(*) FILTER (WHERE distinct_nutriscore_grade > 1)
        AS product_codes_with_changed_nutriscore_grade
FROM (
    SELECT
        product_code,
        count(DISTINCT created_at) AS distinct_created_at,
        count(DISTINCT nutriscore_grade) AS distinct_nutriscore_grade
    FROM staging.product_records
    WHERE is_valid
    GROUP BY product_code
) AS version_variation;
