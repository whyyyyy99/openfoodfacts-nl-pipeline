INSERT INTO affected_codes (product_code)
SELECT product_code
FROM staging.product_records
WHERE product_code IS NOT NULL
UNION
SELECT product_code
FROM mart.products
