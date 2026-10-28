SELECT source_file_name, source_line_number, payload, loaded_at
FROM raw.product_records
ORDER BY source_file_name, source_line_number
