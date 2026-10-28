-- Compare mart rows for the affected codes with an independent recomputation:
-- every column of the expected winner, plus a latest-timestamp rule written
-- with max() instead of DISTINCT ON so one ordering bug cannot hide in both.
WITH expected AS (
    SELECT DISTINCT ON (s.product_code)
        s.product_code,
        s.created_at,
        s.last_modified_at,
        s.rev,
        s.product_name,
        s.brands_tags,
        s.categories_tags,
        s.countries_tags,
        s.nutriscore_grade,
        s.nutriscore_status,
        s.nova_group,
        s.nutrition_basis,
        s.energy_kcal,
        s.energy_kj,
        s.fat_g,
        s.proteins_g,
        s.carbohydrates_g,
        s.sugars_g,
        s.salt_g,
        s.saturated_fat_g,
        s.quality_warnings,
        s.source_file_name,
        s.source_line_number
    FROM staging.product_records AS s
    JOIN affected_codes USING (product_code)
    WHERE s.is_valid
      AND s.last_modified_at IS NOT NULL
    ORDER BY
        s.product_code,
        s.last_modified_at DESC,
        s.source_file_name DESC,
        s.source_line_number DESC
),
actual AS (
    SELECT
        m.product_code,
        m.created_at,
        m.last_modified_at,
        m.rev,
        m.product_name,
        m.brands_tags,
        m.categories_tags,
        m.countries_tags,
        m.nutriscore_grade,
        m.nutriscore_status,
        m.nova_group,
        m.nutrition_basis,
        m.energy_kcal,
        m.energy_kj,
        m.fat_g,
        m.proteins_g,
        m.carbohydrates_g,
        m.sugars_g,
        m.salt_g,
        m.saturated_fat_g,
        m.quality_warnings,
        m.source_file_name,
        m.source_line_number
    FROM mart.products AS m
    JOIN affected_codes USING (product_code)
),
differences AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT SELECT * FROM expected)
),
latest_valid AS (
    SELECT s.product_code, max(s.last_modified_at) AS last_modified_at
    FROM staging.product_records AS s
    JOIN affected_codes USING (product_code)
    WHERE s.is_valid
    GROUP BY s.product_code
)
SELECT
    (SELECT count(*) FROM differences) AS winner_difference_rows,
    (
        SELECT count(*)
        FROM affected_codes
        LEFT JOIN latest_valid AS l USING (product_code)
        LEFT JOIN mart.products AS m USING (product_code)
        WHERE (l.product_code IS NULL) <> (m.product_code IS NULL)
           OR m.last_modified_at IS DISTINCT FROM l.last_modified_at
    ) AS latest_rule_violations
