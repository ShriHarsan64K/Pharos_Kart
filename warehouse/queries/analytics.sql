-- ============================================================
-- PHAROSKART Analytical Queries
-- ============================================================

-- 1. Top 10 customers by total spend
SELECT customer_id, total_spent, total_orders,
       avg_order_value, avg_rating_given, total_posts
FROM dim_customers
ORDER BY total_spent DESC
LIMIT 10;

-- 2. Revenue by product category
SELECT category, total_revenue, total_orders,
       avg_order_value, avg_discount
FROM fact_sales_by_category
ORDER BY total_revenue DESC;

-- 3. Top 10 products by revenue
SELECT product_id, total_sold, total_revenue,
       avg_rating, review_count
FROM fact_product_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. Payment method breakdown
SELECT payment_method, total_transactions,
       total_value,
       ROUND((total_transactions * 100.0 /
           SUM(total_transactions) OVER ())::numeric, 2) AS pct_share
FROM dim_payment_methods
ORDER BY total_transactions DESC;

-- 5. Device traffic analysis
SELECT device, total_sessions,
       ROUND(avg_response_time::numeric, 2) AS avg_ms,
       ROUND((total_sessions * 100.0 /
           SUM(total_sessions) OVER ())::numeric, 2) AS pct_share
FROM dim_traffic_by_device
ORDER BY total_sessions DESC;

-- 6. High value customers (spent > 50000)
SELECT customer_id, total_spent, total_orders,
       categories_purchased, avg_rating_given
FROM dim_customers
WHERE total_spent > 50000
ORDER BY total_spent DESC;

-- 7. Most engaged customers (reviews + social)
SELECT customer_id, total_reviews, avg_rating_given,
       total_posts, total_likes_received, total_shares
FROM dim_customers
ORDER BY (COALESCE(total_reviews,0) + COALESCE(total_posts,0)) DESC
LIMIT 10;

-- 8. Live transaction feed (last 10)
SELECT transaction_id, customer_id, category,
       total_amount, payment_method, status, ingested_at
FROM live_transactions
ORDER BY ingested_at DESC
LIMIT 10;

-- 9. Real-time revenue by category (from live stream)
SELECT category,
       COUNT(*) AS live_orders,
       ROUND(SUM(total_amount)::numeric, 2) AS live_revenue
FROM live_transactions
WHERE status = 'completed'
GROUP BY category
ORDER BY live_revenue DESC;

-- 10. Sentiment breakdown from live social stream
SELECT sentiment,
       COUNT(*) AS post_count,
       ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::numeric, 2) AS pct
FROM live_social
GROUP BY sentiment
ORDER BY post_count DESC;
