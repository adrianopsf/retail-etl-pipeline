-- =============================================================
-- Analytics layer
-- Business-ready tables built on top of the staging schema.
-- Run AFTER the staging tables have been populated by the ETL.
-- =============================================================

-- Drop views first to allow clean recreation
DROP TABLE IF EXISTS analytics.agg_monthly_sales;
DROP TABLE IF EXISTS analytics.fact_orders;
DROP TABLE IF EXISTS analytics.dim_customers;
DROP TABLE IF EXISTS analytics.dim_products;

-- -----------------------------------------------------------
-- fact_orders: one row per delivered order
-- -----------------------------------------------------------
CREATE TABLE analytics.fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.order_month,
    o.delivery_days,
    o.is_late_delivery,
    o.total_items,
    o.total_order_value,
    COALESCE(r.review_score, -1)    AS review_score
FROM staging.orders o
LEFT JOIN (
    -- Take the most recent review per order
    SELECT DISTINCT ON (order_id)
        order_id,
        review_score
    FROM staging.order_reviews
    ORDER BY order_id, review_answer_timestamp DESC
) r USING (order_id);

ALTER TABLE analytics.fact_orders ADD PRIMARY KEY (order_id);
CREATE INDEX idx_fact_orders_month ON analytics.fact_orders (order_month);
CREATE INDEX idx_fact_orders_customer ON analytics.fact_orders (customer_id);

-- -----------------------------------------------------------
-- dim_customers: one row per unique customer
-- -----------------------------------------------------------
CREATE TABLE analytics.dim_customers AS
SELECT
    customer_unique_id,
    customer_id,
    customer_city,
    customer_state,
    customer_segment,
    total_orders,
    total_spent,
    first_order_date,
    last_order_date
FROM staging.customers;

ALTER TABLE analytics.dim_customers ADD PRIMARY KEY (customer_unique_id);
CREATE INDEX idx_dim_customers_state ON analytics.dim_customers (customer_state);
CREATE INDEX idx_dim_customers_segment ON analytics.dim_customers (customer_segment);

-- -----------------------------------------------------------
-- dim_products: one row per unique product
-- -----------------------------------------------------------
CREATE TABLE analytics.dim_products AS
SELECT
    product_id,
    product_category_name_english   AS category,
    category_slug,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    total_sold,
    avg_price
FROM staging.products;

ALTER TABLE analytics.dim_products ADD PRIMARY KEY (product_id);
CREATE INDEX idx_dim_products_category ON analytics.dim_products (category_slug);

-- -----------------------------------------------------------
-- agg_monthly_sales: revenue and order counts per month
-- -----------------------------------------------------------
CREATE TABLE analytics.agg_monthly_sales AS
SELECT
    fo.order_month,
    COUNT(fo.order_id)              AS total_orders,
    SUM(fo.total_order_value)       AS total_revenue,
    AVG(fo.total_order_value)       AS avg_order_value,
    AVG(fo.delivery_days)
        FILTER (WHERE fo.delivery_days >= 0)
                                    AS avg_delivery_days,
    SUM(CASE WHEN fo.is_late_delivery THEN 1 ELSE 0 END)
                                    AS late_deliveries,
    COUNT(DISTINCT fo.customer_id)  AS unique_customers
FROM analytics.fact_orders fo
GROUP BY fo.order_month
ORDER BY fo.order_month;

ALTER TABLE analytics.agg_monthly_sales ADD PRIMARY KEY (order_month);
