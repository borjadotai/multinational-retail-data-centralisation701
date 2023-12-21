DELETE FROM dim_store_details
WHERE latitude IS NOT NULL AND latitude::TEXT !~ '^[+-]?([0-9]*[.])?[0-9]+$';

DELETE FROM dim_store_details
WHERE lat IS NOT NULL AND lat::TEXT !~ '^[+-]?([0-9]*[.])?[0-9]+$';

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE DOUBLE PRECISION USING latitude::DOUBLE PRECISION,
ALTER COLUMN lat TYPE DOUBLE PRECISION USING lat::DOUBLE PRECISION;

UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat)
WHERE latitude IS NULL OR lat IS NOT NULL;

ALTER TABLE dim_store_details
DROP COLUMN lat;

SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

DELETE FROM dim_store_details
WHERE staff_numbers !~ '^\d+$';

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(11),
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

