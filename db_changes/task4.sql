-- Already taken care of £ sign on my data cleaning on the python side

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE 'Unknown'  -- Handles cases where weight might be NULL or out of expected range
END;
