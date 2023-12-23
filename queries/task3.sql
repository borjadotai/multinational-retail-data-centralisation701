SELECT dt.month, COUNT(*) AS total_sales
FROM orders_table AS ot
JOIN dim_date_times AS dt ON ot.date_uuid = dt.date_uuid
GROUP BY dt.month
ORDER BY total_sales DESC;