-- Recompute the winner of every affected code from its full staging history.
-- The column lists must stay aligned with mart.products and
-- check_affected_winners.sql; tests/test_transform.py enforces this.
INSERT INTO mart.products (
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
    energy_kcal,
    energy_kj,
    fat_g,
    proteins_g,
    carbohydrates_g,
    sugars_g,
    salt_g,
    saturated_fat_g,
    quality_warnings,
    source_file_name,
    source_line_number
)
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
JOIN affected_codes AS a USING (product_code)
WHERE s.is_valid
  AND s.last_modified_at IS NOT NULL
ORDER BY
    s.product_code,
    s.last_modified_at DESC,
    s.source_file_name DESC,
    s.source_line_number DESC
