SELECT
    (SELECT count(*) FROM raw.product_records) AS raw_rows,
    (SELECT count(*) FROM staging.product_records) AS staging_rows,
    (SELECT count(*) FROM staging.product_records WHERE is_valid) AS valid_rows,
    (SELECT count(*) FROM staging.product_records WHERE NOT is_valid) AS invalid_rows,
    (
        SELECT count(DISTINCT product_code)
        FROM staging.product_records
        WHERE is_valid
    ) AS expected_mart_rows,
    (SELECT count(*) FROM mart.products) AS actual_mart_rows
