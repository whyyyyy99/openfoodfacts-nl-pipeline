SELECT load_status, loaded_row_count
FROM raw.source_files
WHERE source_file_name = %s
FOR UPDATE
