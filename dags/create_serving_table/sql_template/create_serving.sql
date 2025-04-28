DROP TABLE IF EXISTS serving.fact_sales_weather;
CREATE TABLE serving.fact_sales_weather AS
SELECT
    t.trans_date AS weather_date,
    s.store_id,
    s.store_city,
    s.store_country,
    -- Tính tier hoặc lấy trực tiếp nếu có cột tier
    CASE 
      WHEN s.store_emp_num >= 50 THEN 'Large'
      WHEN s.store_emp_num >= 20 THEN 'Medium'
      ELSE 'Small'
    END AS store_tier,
    p.prod_id AS product_id,
    p.prod_cat AS product_category,
    w.temperature_avg,
    w.precipitation,
    SUM(t.total_line) AS sales_total,
    SUM(t.quantity) AS quantity_total,
    COUNT(DISTINCT t.trans_id) AS transaction_count
FROM stg.transactions_temp t
JOIN stg.stores_temp s ON t.store_id = s.store_id
JOIN stg.products_temp p ON t.prod_id = p.prod_id
LEFT JOIN stg.weather_temp w
    ON w.weather_date = DATE(t.trans_date)
    AND ROUND(w.latitude, 4) = ROUND(s.store_lat, 4)
    AND ROUND(w.longitude, 4) = ROUND(s.store_long, 4)
GROUP BY
    t.trans_date, s.store_id, s.store_city, s.store_country, store_tier,
    p.prod_id, p.prod_cat, w.temperature_avg, w.precipitation
;

-- Xoá các bảng temp staging
DO $$
BEGIN
    DROP TABLE IF EXISTS stg.customers_temp;
    DROP TABLE IF EXISTS stg.discounts_temp;
    DROP TABLE IF EXISTS stg.employees_temp;
    DROP TABLE IF EXISTS stg.products_temp;
    DROP TABLE IF EXISTS stg.stores_temp;
    DROP TABLE IF EXISTS stg.transactions_temp;
    DROP TABLE IF EXISTS stg.weather_temp;
END $$;