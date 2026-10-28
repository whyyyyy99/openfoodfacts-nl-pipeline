UPDATE raw.source_files
SET load_started_at = clock_timestamp(),
    load_completed_at = NULL,
    loaded_row_count = NULL
WHERE source_file_name = %s
