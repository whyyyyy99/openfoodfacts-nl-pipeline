SELECT
    COALESCE(
        (
            SELECT array_agg(source_file_name ORDER BY source_file_name)
            FROM raw.source_files
            WHERE load_status = 'loaded'
        ),
        ARRAY[]::text[]
    ) AS loaded_source_files,
    (SELECT count(*) FROM raw.product_records) AS raw_rows,
    (SELECT count(*) FROM staging.product_records) AS staging_rows,
    (SELECT count(*) FROM staging.product_records WHERE is_valid) AS valid_rows,
    (SELECT count(*) FROM staging.product_records WHERE NOT is_valid) AS invalid_rows,
    (
        (SELECT count(*) FROM staging.product_records WHERE is_valid)
        -
        (SELECT count(DISTINCT product_code)
         FROM staging.product_records
         WHERE is_valid)
    ) AS non_winning_valid_rows,
    (SELECT count(*) FROM mart.products) AS mart_rows,
    (
        SELECT count(*)
        FROM (
            SELECT product_code
            FROM staging.product_records
            WHERE is_valid
            GROUP BY product_code
            HAVING count(DISTINCT source_file_name) > 1
        ) AS cross_file_products
    ) AS cross_file_product_count
