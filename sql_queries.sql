-- sql_queries.sql
-- Business Analysis SQL Queries on cleaned_food_orders table

-- Query 1: What is the total order value?
SELECT 
    ROUND(SUM(order_value), 2) AS total_order_value
FROM cleaned_food_orders
WHERE order_value IS NOT NULL;

-- Query 2: What is the average order value?
SELECT 
    ROUND(AVG(order_value), 2) AS average_order_value
FROM cleaned_food_orders
WHERE order_value IS NOT NULL;

-- Query 3: Which payment method has the highest number of orders?
SELECT 
    payment_method, 
    COUNT(*) AS total_orders
FROM cleaned_food_orders
WHERE payment_method != 'Unknown'
GROUP BY payment_method
ORDER BY total_orders DESC
LIMIT 1;

-- Query 4: Which order channel has the highest number of orders?
SELECT 
    order_channel, 
    COUNT(*) AS total_orders
FROM cleaned_food_orders
GROUP BY order_channel
ORDER BY total_orders DESC
LIMIT 1;

-- Query 5: What is the average delivery time in minutes?
SELECT 
    ROUND(AVG(delivery_time_minutes), 2) AS average_delivery_time_minutes
FROM cleaned_food_orders
WHERE delivery_time_minutes IS NOT NULL;

-- Query 6: What is the total platform net revenue according to the specified formula?
-- Formula: Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
-- (Only across records where all 4 components are available)
SELECT 
    ROUND(SUM(platform_net_revenue), 2) AS total_platform_net_revenue,
    COUNT(platform_net_revenue) AS contributing_orders_count
FROM cleaned_food_orders
WHERE platform_net_revenue IS NOT NULL;

