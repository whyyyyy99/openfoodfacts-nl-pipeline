SELECT s.source_file_name, s.product_code, count(*) AS row_count
FROM staging.product_records AS s
JOIN affected_codes AS a USING (product_code)
WHERE s.is_valid
GROUP BY s.source_file_name, s.product_code
HAVING count(*) > 1
ORDER BY s.source_file_name, s.product_code
LIMIT 10
