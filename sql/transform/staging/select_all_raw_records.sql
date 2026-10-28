SELECT source_file_name, source_line_number, loaded_at, payload
FROM raw.product_records
ORDER BY source_file_name, source_line_number
