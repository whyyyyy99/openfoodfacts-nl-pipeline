SELECT source_file_name
FROM raw.source_files
WHERE load_status = 'loaded'
ORDER BY source_file_name
