BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.source_files (
    source_file_name TEXT NOT NULL,
    load_status TEXT NOT NULL DEFAULT 'pending',
    load_started_at TIMESTAMPTZ,
    load_completed_at TIMESTAMPTZ,
    loaded_row_count BIGINT,
    CONSTRAINT pk_source_files PRIMARY KEY (source_file_name),
    CONSTRAINT ck_source_files_load_status
        CHECK (load_status IN ('pending', 'loaded')),
    CONSTRAINT ck_source_files_loaded_row_count
        CHECK (loaded_row_count IS NULL OR loaded_row_count >= 0),
    CONSTRAINT ck_source_files_loaded_metadata
        CHECK (
            load_status = 'pending'
            OR (
                load_started_at IS NOT NULL
                AND load_completed_at IS NOT NULL
                AND load_completed_at >= load_started_at
                AND loaded_row_count IS NOT NULL
            )
        )
);

CREATE TABLE IF NOT EXISTS raw.product_records (
    source_file_name TEXT NOT NULL,
    source_line_number BIGINT NOT NULL,
    payload JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_product_records
        PRIMARY KEY (source_file_name, source_line_number),
    CONSTRAINT fk_product_records_source_file
        FOREIGN KEY (source_file_name)
        REFERENCES raw.source_files (source_file_name)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT ck_product_records_source_line_number
        CHECK (source_line_number > 0)
);

CREATE TABLE IF NOT EXISTS staging.product_records (
    source_file_name TEXT NOT NULL,
    source_line_number BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    product_code TEXT,
    created_at TIMESTAMPTZ,
    last_modified_at TIMESTAMPTZ,
    rev BIGINT,
    product_name TEXT,
    brands_tags TEXT[],
    categories_tags TEXT[],
    countries_tags TEXT[],
    nutriscore_grade TEXT,
    nutriscore_status TEXT NOT NULL,
    nova_group INTEGER,
    nutrition_basis TEXT,
    energy_kcal NUMERIC,
    energy_kj NUMERIC,
    fat_g NUMERIC,
    proteins_g NUMERIC,
    carbohydrates_g NUMERIC,
    sugars_g NUMERIC,
    salt_g NUMERIC,
    saturated_fat_g NUMERIC,
    validation_errors TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    quality_warnings TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    mart_applied BOOLEAN NOT NULL DEFAULT false,
    is_valid BOOLEAN GENERATED ALWAYS AS (
        cardinality(validation_errors) = 0
    ) STORED,
    CONSTRAINT pk_product_records
        PRIMARY KEY (source_file_name, source_line_number),
    CONSTRAINT fk_product_records_raw_record
        FOREIGN KEY (source_file_name, source_line_number)
        REFERENCES raw.product_records (source_file_name, source_line_number)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT ck_product_records_nutrition_basis
        CHECK (nutrition_basis IS NULL OR nutrition_basis IN ('100g', '100ml')),
    CONSTRAINT ck_product_records_nutriscore
        CHECK (
            nutriscore_status IN (
                'available', 'unknown', 'not_applicable', 'missing', 'unexpected'
            )
            AND (
                (
                    nutriscore_status = 'available'
                    AND nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
                )
                OR (
                    nutriscore_status <> 'available'
                    AND nutriscore_grade IS NULL
                )
            )
        )
);

CREATE TABLE IF NOT EXISTS mart.products (
    product_code TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    last_modified_at TIMESTAMPTZ NOT NULL,
    rev BIGINT,
    product_name TEXT,
    brands_tags TEXT[],
    categories_tags TEXT[],
    countries_tags TEXT[],
    nutriscore_grade TEXT,
    nutriscore_status TEXT NOT NULL,
    nova_group INTEGER,
    nutrition_basis TEXT,
    energy_kcal NUMERIC,
    energy_kj NUMERIC,
    fat_g NUMERIC,
    proteins_g NUMERIC,
    carbohydrates_g NUMERIC,
    sugars_g NUMERIC,
    salt_g NUMERIC,
    saturated_fat_g NUMERIC,
    quality_warnings TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    source_file_name TEXT NOT NULL,
    source_line_number BIGINT NOT NULL,
    CONSTRAINT pk_products PRIMARY KEY (product_code),
    CONSTRAINT ck_products_product_code
        CHECK (product_code ~ '^[0-9]+$'),
    CONSTRAINT fk_products_staging_record
        FOREIGN KEY (source_file_name, source_line_number)
        REFERENCES staging.product_records (source_file_name, source_line_number)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT ck_products_nutrition_basis
        CHECK (nutrition_basis IS NULL OR nutrition_basis IN ('100g', '100ml')),
    CONSTRAINT ck_products_nutriscore
        CHECK (
            nutriscore_status IN (
                'available', 'unknown', 'not_applicable', 'missing', 'unexpected'
            )
            AND (
                (
                    nutriscore_status = 'available'
                    AND nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
                )
                OR (
                    nutriscore_status <> 'available'
                    AND nutriscore_grade IS NULL
                )
            )
        )
);

CREATE TABLE IF NOT EXISTS mart.refresh_log (
    refresh_id BIGINT GENERATED ALWAYS AS IDENTITY,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    refresh_mode TEXT NOT NULL,
    affected_product_count BIGINT NOT NULL,
    latest_source_file_name TEXT,
    latest_source_window_ended_at TIMESTAMPTZ,
    latest_product_modified_at TIMESTAMPTZ,
    loaded_source_file_count BIGINT NOT NULL,
    raw_row_count BIGINT NOT NULL,
    staging_row_count BIGINT NOT NULL,
    mart_row_count BIGINT NOT NULL,
    CONSTRAINT pk_refresh_log PRIMARY KEY (refresh_id),
    CONSTRAINT ck_refresh_log_mode
        CHECK (
            refresh_mode IN (
                'mart_incremental',
                'mart_full_refresh',
                'staging_and_mart_rebuild'
            )
        ),
    CONSTRAINT ck_refresh_log_counts
        CHECK (
            affected_product_count >= 0
            AND loaded_source_file_count >= 0
            AND raw_row_count >= 0
            AND staging_row_count >= 0
            AND mart_row_count >= 0
        ),
    CONSTRAINT ck_refresh_log_source_window
        CHECK (
            (latest_source_file_name IS NULL)
            = (latest_source_window_ended_at IS NULL)
        )
);

CREATE INDEX IF NOT EXISTS ix_product_records_product_version
    ON staging.product_records (
        product_code,
        last_modified_at DESC,
        rev DESC
    )
    WHERE is_valid;

CREATE INDEX IF NOT EXISTS ix_product_records_mart_pending
    ON staging.product_records (product_code)
    WHERE NOT mart_applied;

CREATE INDEX IF NOT EXISTS ix_products_source_record
    ON mart.products (source_file_name, source_line_number);

COMMIT;
