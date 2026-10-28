-- Stable database-side counts for README run evidence and file-profile checks.
SELECT
    (SELECT count(*) FROM raw.product_records) AS raw_rows,
    (SELECT count(*) FROM staging.product_records) AS staging_rows,
    (
        SELECT count(*)
        FROM staging.product_records
        WHERE is_valid
    ) AS valid_rows,
    (
        SELECT count(*)
        FROM staging.product_records
        WHERE NOT is_valid
    ) AS invalid_rows,
    (
        SELECT count(*)
        FROM raw.product_records
        WHERE payload->>'code' LIKE '0%'
    ) AS raw_leading_zero_rows,
    (
        SELECT count(*)
        FROM staging.product_records
        WHERE product_code LIKE '0%'
    ) AS staging_leading_zero_rows,
    (SELECT count(*) FROM mart.products) AS mart_rows,
    (
        SELECT count(*)
        FROM (
            SELECT product_code
            FROM staging.product_records
            WHERE is_valid
            GROUP BY product_code
            HAVING count(DISTINCT source_file_name) > 1
        ) AS cross_file_products
    ) AS cross_file_products;

-- Contract checks: complete mart grain, exact expected winner, and provenance.
WITH expected_winners AS (
    SELECT DISTINCT ON (product_code)
        product_code,
        source_file_name,
        source_line_number
    FROM staging.product_records
    WHERE is_valid
      AND last_modified_at IS NOT NULL
    ORDER BY
        product_code,
        last_modified_at DESC,
        source_file_name DESC,
        source_line_number DESC
),
winner_differences AS (
    (
        SELECT product_code, source_file_name, source_line_number
        FROM expected_winners
        EXCEPT
        SELECT product_code, source_file_name, source_line_number
        FROM mart.products
    )
    UNION ALL
    (
        SELECT product_code, source_file_name, source_line_number
        FROM mart.products
        EXCEPT
        SELECT product_code, source_file_name, source_line_number
        FROM expected_winners
    )
)
SELECT
    (
        SELECT count(DISTINCT product_code)
        FROM staging.product_records
        WHERE is_valid
    ) AS distinct_valid_codes,
    (SELECT count(*) FROM mart.products) AS mart_rows,
    (
        SELECT count(*)
        FROM (
            SELECT product_code
            FROM staging.product_records
            WHERE is_valid
            EXCEPT
            SELECT product_code
            FROM mart.products
        ) AS missing_codes
    ) AS valid_codes_missing_from_mart,
    (SELECT count(*) FROM winner_differences) AS mart_winner_differences,
    (
        SELECT count(*)
        FROM staging.product_records AS s
        LEFT JOIN raw.product_records AS r
          USING (source_file_name, source_line_number)
        WHERE r.source_file_name IS NULL
    ) AS staging_without_raw,
    (
        SELECT count(*)
        FROM mart.products AS m
        LEFT JOIN staging.product_records AS s
          USING (source_file_name, source_line_number)
        WHERE s.source_file_name IS NULL
    ) AS mart_without_staging;

-- Analyst-facing freshness: build time and source/data coverage are distinct.
SELECT
    refreshed_at,
    refresh_mode,
    affected_product_count,
    latest_source_file_name,
    latest_source_window_ended_at,
    latest_product_modified_at,
    loaded_source_file_count,
    raw_row_count,
    staging_row_count,
    mart_row_count
FROM mart.refresh_log
ORDER BY refresh_id DESC
LIMIT 1;

-- How many products appeared in more than one loaded file, by file count.
SELECT
    file_count,
    count(*) AS products
FROM (
    SELECT product_code, count(DISTINCT source_file_name) AS file_count
    FROM staging.product_records
    WHERE is_valid
    GROUP BY product_code
) AS appearances
WHERE file_count > 1
GROUP BY file_count
ORDER BY file_count;
