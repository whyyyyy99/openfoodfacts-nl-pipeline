UPDATE staging.product_records
SET mart_applied = true
WHERE NOT mart_applied
