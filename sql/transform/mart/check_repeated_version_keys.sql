SELECT s.product_code, s.last_modified_at, count(*) AS row_count
FROM staging.product_records AS s
JOIN affected_codes AS a USING (product_code)
WHERE s.is_valid
GROUP BY s.product_code, s.last_modified_at
HAVING count(*) > 1
ORDER BY s.product_code, s.last_modified_at
LIMIT 10
