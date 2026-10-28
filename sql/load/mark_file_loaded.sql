UPDATE raw.source_files
SET load_status = 'loaded',
    load_completed_at = clock_timestamp(),
    loaded_row_count = %s
WHERE source_file_name = %s
