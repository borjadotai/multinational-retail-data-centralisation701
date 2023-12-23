SELECT 
    dsd.store_type, 
    SUM(dp.product_price * ot.product_quantity) AS total_value_sold
FROM 
    orders_table ot
JOIN 
    dim_products dp ON ot.product_code = dp.product_code
JOIN 
    dim_store_details dsd ON ot.store_code = dsd.store_code
GROUP BY 
    dsd.store_type
ORDER BY 
    total_value_sold DESC;
