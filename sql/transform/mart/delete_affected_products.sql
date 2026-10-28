DELETE FROM mart.products AS m
USING affected_codes AS a
WHERE m.product_code = a.product_code
