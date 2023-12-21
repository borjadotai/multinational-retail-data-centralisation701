SELECT MAX(LENGTH("EAN")) FROM dim_products; 
-- 17
SELECT MAX(LENGTH(product_code)) FROM dim_products;
-- 11
SELECT MAX(LENGTH(weight_class)) FROM dim_products;
-- 14

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

DELETE FROM dim_products
WHERE still_available NOT IN ('Still_Available', 'Removed');

UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_Available' THEN true
    WHEN still_available = 'Removed' THEN false
END
WHERE still_available IN ('Still_Available', 'Removed');

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL,
ALTER COLUMN weight_class TYPE VARCHAR(14);