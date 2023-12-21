DELETE FROM dim_date_times
WHERE LENGTH(month) > 2 OR LENGTH(year) > 4 OR LENGTH(day) > 2;

SELECT MAX(LENGTH(time_period)) FROM dim_date_times;
-- 10
SELECT MAX(LENGTH(month)) FROM dim_date_times;
-- 2
SELECT MAX(LENGTH(year)) FROM dim_date_times;
-- 4
SELECT MAX(LENGTH(day)) FROM dim_date_times;
-- 2

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
