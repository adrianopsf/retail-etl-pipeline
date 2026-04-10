-- Analytics layer: business-ready aggregations built on top of staging.

CREATE TABLE IF NOT EXISTS analytics.fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.delivery_days,
    o.is_late,
    SUM(oi.price)           AS total_revenue,
    SUM(oi.freight_value)   AS total_freight,
    COUNT(oi.order_item_id) AS item_count
FROM staging.orders o
LEFT JOIN staging.order_items oi USING (order_id)
GROUP BY
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.delivery_days,
    o.is_late;

CREATE TABLE IF NOT EXISTS analytics.dim_customers AS
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM staging.customers;

CREATE TABLE IF NOT EXISTS analytics.dim_products AS
SELECT
    product_id,
    product_category_name_english AS category,
    product_weight_g
FROM staging.products;
