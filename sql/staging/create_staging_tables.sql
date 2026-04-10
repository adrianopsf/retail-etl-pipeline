-- =============================================================
-- Staging layer
-- Raw data loaded directly from the Olist CSV files.
-- Column types match source CSVs; no business logic applied here.
-- =============================================================

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id                        TEXT        PRIMARY KEY,
    customer_id                     TEXT        NOT NULL,
    order_status                    TEXT,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    -- Derived fields added by the transformer
    order_month                     TEXT,
    delivery_days                   INTEGER,
    is_late_delivery                BOOLEAN,
    total_items                     INTEGER,
    total_order_value               NUMERIC(12, 2)
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id                 TEXT    PRIMARY KEY,
    customer_unique_id          TEXT    NOT NULL,
    customer_zip_code_prefix    TEXT,
    customer_city               TEXT,
    customer_state              CHAR(2),
    -- Derived fields added by the transformer
    total_orders                INTEGER,
    total_spent                 NUMERIC(12, 2),
    customer_segment            TEXT,
    first_order_date            TIMESTAMP,
    last_order_date             TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id                      TEXT    PRIMARY KEY,
    product_category_name           TEXT,
    product_category_name_english   TEXT,
    category_slug                   TEXT,
    product_name_length             INTEGER,
    product_description_length      INTEGER,
    product_photos_qty              INTEGER,
    product_weight_g                INTEGER,
    product_length_cm               INTEGER,
    product_height_cm               INTEGER,
    product_width_cm                INTEGER,
    -- Derived fields added by the transformer
    total_sold                      INTEGER,
    avg_price                       NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id            TEXT,
    order_item_id       INTEGER,
    product_id          TEXT,
    seller_id           TEXT,
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10, 2),
    freight_value       NUMERIC(10, 2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS staging.order_payments (
    order_id                TEXT,
    payment_sequential      INTEGER,
    payment_type            TEXT,
    payment_installments    INTEGER,
    payment_value           NUMERIC(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS staging.order_reviews (
    review_id                   TEXT    PRIMARY KEY,
    order_id                    TEXT,
    review_score                SMALLINT    CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title        TEXT,
    review_comment_message      TEXT,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id               TEXT    PRIMARY KEY,
    seller_zip_code_prefix  TEXT,
    seller_city             TEXT,
    seller_state            CHAR(2)
);
