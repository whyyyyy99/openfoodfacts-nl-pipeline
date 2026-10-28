WITH official_loaded_files AS (
    SELECT
        source_file_name,
        substring(
            source_file_name
            FROM '^openfoodfacts_products_[0-9]+_([0-9]+)[.]json[.]gz$'
        )::double precision AS window_end_epoch
    FROM raw.source_files
    WHERE load_status = 'loaded'
      AND source_file_name
          ~ '^openfoodfacts_products_[0-9]+_[0-9]+[.]json[.]gz$'
),
latest_source AS (
    SELECT source_file_name, window_end_epoch
    FROM official_loaded_files
    ORDER BY window_end_epoch DESC, source_file_name DESC
    LIMIT 1
)
INSERT INTO mart.refresh_log (
    refresh_mode,
    affected_product_count,
    latest_source_file_name,
    latest_source_window_ended_at,
    latest_product_modified_at,
    loaded_source_file_count,
    raw_row_count,
    staging_row_count,
    mart_row_count
)
SELECT
    %s,
    %s,
    (SELECT source_file_name FROM latest_source),
    (SELECT to_timestamp(window_end_epoch) FROM latest_source),
    (SELECT max(last_modified_at) FROM mart.products),
    (SELECT count(*) FROM raw.source_files WHERE load_status = 'loaded'),
    (SELECT count(*) FROM raw.product_records),
    (SELECT count(*) FROM staging.product_records),
    (SELECT count(*) FROM mart.products)
RETURNING mart_row_count
