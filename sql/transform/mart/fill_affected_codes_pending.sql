INSERT INTO affected_codes (product_code)
SELECT DISTINCT product_code
FROM staging.product_records
WHERE NOT mart_applied
  AND product_code IS NOT NULL
