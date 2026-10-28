INSERT INTO raw.source_files (source_file_name)
VALUES (%s)
ON CONFLICT (source_file_name) DO NOTHING
