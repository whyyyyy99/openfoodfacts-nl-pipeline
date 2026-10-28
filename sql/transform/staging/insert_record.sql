INSERT INTO staging.product_records (
    source_file_name, source_line_number, loaded_at, product_code,
    created_at, last_modified_at, rev, product_name, brands_tags,
    categories_tags, countries_tags, nutriscore_grade, nutriscore_status, nova_group,
    nutrition_basis, energy_kcal, energy_kj, fat_g, proteins_g,
    carbohydrates_g, sugars_g, salt_g, saturated_fat_g,
    validation_errors, quality_warnings
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (source_file_name, source_line_number) DO NOTHING
