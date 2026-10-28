SELECT r.source_file_name, r.source_line_number, r.loaded_at, r.payload
FROM raw.product_records AS r
LEFT JOIN staging.product_records AS s
  ON s.source_file_name = r.source_file_name
 AND s.source_line_number = r.source_line_number
WHERE s.source_file_name IS NULL
ORDER BY r.source_file_name, r.source_line_number
