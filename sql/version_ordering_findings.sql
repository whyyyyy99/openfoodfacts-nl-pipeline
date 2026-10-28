-- Would another ordering field pick a different winner? Compared over every
-- product that has more than one valid staged version.
WITH versions AS (
    SELECT
        s.product_code,
        s.source_file_name || ':' || s.source_line_number AS row_id,
        s.last_modified_at,
        s.rev,
        (r.payload ->> 'last_updated_t')::bigint AS last_updated_t,
        s.source_file_name,
        s.source_line_number
    FROM staging.product_records AS s
    JOIN raw.product_records AS r USING (source_file_name, source_line_number)
    WHERE s.is_valid
      AND s.product_code IN (
          SELECT product_code
          FROM staging.product_records
          WHERE is_valid
          GROUP BY product_code
          HAVING count(*) > 1
      )
),
winners AS (
    SELECT
        product_code,
        (array_agg(row_id ORDER BY last_modified_at DESC))[1] AS by_last_modified,
        (array_agg(row_id ORDER BY rev DESC))[1] AS by_rev,
        (array_agg(row_id ORDER BY last_updated_t DESC))[1] AS by_last_updated,
        (array_agg(row_id ORDER BY source_file_name DESC, source_line_number DESC))[1]
            AS by_latest_file
    FROM versions
    GROUP BY product_code
)
SELECT
    count(*) AS multi_version_products,
    count(*) FILTER (WHERE by_rev <> by_last_modified) AS rev_disagrees,
    count(*) FILTER (WHERE by_last_updated <> by_last_modified) AS last_updated_disagrees,
    count(*) FILTER (WHERE by_latest_file <> by_last_modified) AS latest_file_disagrees
FROM winners;

-- Records whose last_updated_t is later than last_modified_t (a non-user update
-- without a new modification time) and how far apart the two are.
SELECT
    count(*) AS rows_updated_after_modified,
    min(gap_seconds) AS min_gap_seconds,
    max(gap_seconds) AS max_gap_seconds,
    count(*) FILTER (WHERE last_updated_by IS NULL) AS without_last_updated_by
FROM (
    SELECT
        (payload ->> 'last_updated_t')::bigint
            - (payload ->> 'last_modified_t')::bigint AS gap_seconds,
        payload ->> 'last_updated_by' AS last_updated_by
    FROM raw.product_records
) AS timestamps
WHERE gap_seconds > 0;
