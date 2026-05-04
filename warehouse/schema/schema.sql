-- ============================================================
-- PHAROSKART Star Schema
-- PostgreSQL 16
-- ============================================================

-- Dimension: Unified Customer 360 Profile
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id          VARCHAR(20) PRIMARY KEY,
    total_sessions       INT,
    unique_pages_visited INT,
    avg_response_time    FLOAT,
    device_count         INT,
    total_orders         INT,
    total_spent          FLOAT,
    avg_order_value      FLOAT,
    max_order_value      FLOAT,
    categories_purchased INT,
    total_reviews        INT,
    avg_rating_given     FLOAT,
    total_helpful_votes  INT,
    total_posts          INT,
    platforms_used       INT,
    total_likes_received BIGINT,
    total_shares         BIGINT
);

-- Dimension: Payment Methods
CREATE TABLE IF NOT EXISTS dim_payment_methods (
    payment_method     VARCHAR(50) PRIMARY KEY,
    total_transactions INT,
    total_value        FLOAT
);

-- Dimension: Traffic by Device
CREATE TABLE IF NOT EXISTS dim_traffic_by_device (
    device            VARCHAR(20) PRIMARY KEY,
    total_sessions    INT,
    avg_response_time FLOAT
);

-- Fact: Sales by Category
CREATE TABLE IF NOT EXISTS fact_sales_by_category (
    category        VARCHAR(50) PRIMARY KEY,
    total_orders    INT,
    total_revenue   FLOAT,
    avg_order_value FLOAT,
    avg_discount    FLOAT
);

-- Fact: Product Performance
CREATE TABLE IF NOT EXISTS fact_product_performance (
    product_id    VARCHAR(20) PRIMARY KEY,
    total_sold    INT,
    total_revenue FLOAT,
    avg_rating    FLOAT,
    review_count  INT
);

-- Live: Real-time Transactions (from Kafka)
CREATE TABLE IF NOT EXISTS live_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id    VARCHAR(20),
    product_id     VARCHAR(20),
    category       VARCHAR(50),
    quantity       INT,
    unit_price     FLOAT,
    discount_pct   FLOAT,
    total_amount   FLOAT,
    payment_method VARCHAR(50),
    status         VARCHAR(20),
    timestamp      TIMESTAMP,
    ingested_at    TIMESTAMP DEFAULT NOW()
);

-- Live: Real-time Web Logs (from Kafka)
CREATE TABLE IF NOT EXISTS live_weblogs (
    session_id       VARCHAR(100) PRIMARY KEY,
    customer_id      VARCHAR(20),
    ip_address       VARCHAR(20),
    method           VARCHAR(10),
    page             VARCHAR(100),
    status_code      INT,
    response_time_ms INT,
    device           VARCHAR(20),
    country          VARCHAR(10),
    timestamp        TIMESTAMP,
    ingested_at      TIMESTAMP DEFAULT NOW()
);

-- Live: Real-time Reviews (from Kafka)
CREATE TABLE IF NOT EXISTS live_reviews (
    review_id         VARCHAR(50) PRIMARY KEY,
    customer_id       VARCHAR(20),
    product_id        VARCHAR(20),
    rating            INT,
    review_text       TEXT,
    verified_purchase BOOLEAN,
    timestamp         TIMESTAMP,
    ingested_at       TIMESTAMP DEFAULT NOW()
);

-- Live: Real-time Social Media (from Kafka)
CREATE TABLE IF NOT EXISTS live_social (
    post_id     VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(20),
    platform    VARCHAR(20),
    content     TEXT,
    sentiment   VARCHAR(20),
    likes       INT,
    timestamp   TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
);
