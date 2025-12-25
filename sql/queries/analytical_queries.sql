/* =====================================================
Query 1: Top 10 Products by Revenue
Purpose: Identify best-selling products
===================================================== */
SELECT
    p.product_name,
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    AVG(f.unit_price) AS avg_price
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
  ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

/* =====================================================
Query 2: Monthly Sales Trend
Purpose: Analyze revenue over time
===================================================== */
SELECT
    d.year || '-' || LPAD(d.month::TEXT,2,'0') AS year_month,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    AVG(f.line_total) AS average_order_value,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
  ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

/* =====================================================
Query 3: Customer Segmentation Analysis
Purpose: Group customers by spending
===================================================== */
WITH customer_totals AS (
    SELECT
        customer_key,
        SUM(line_total) AS total_spent,
        COUNT(DISTINCT transaction_id) AS txn_count
    FROM warehouse.fact_sales
    GROUP BY customer_key
)
SELECT
    CASE
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent < 5000 THEN '$1,000-$5,000'
        WHEN total_spent < 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    AVG(total_spent / txn_count) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment;

/* =====================================================
Query 4: Category Performance
Purpose: Compare product categories
===================================================== */
SELECT
    p.category,
    SUM(f.line_total) AS total_revenue,
    SUM(f.profit) AS total_profit,
    (SUM(f.profit) / NULLIF(SUM(f.line_total),0)) * 100 AS profit_margin_pct,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
  ON f.product_key = p.product_key
GROUP BY p.category;

/* =====================================================
Query 5: Payment Method Distribution
Purpose: Understand payment preferences
===================================================== */
SELECT
    pm.payment_method_name AS payment_method,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    SUM(f.line_total) AS total_revenue,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_transactions,
    SUM(f.line_total) * 100.0 / SUM(SUM(f.line_total)) OVER () AS pct_of_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_payment_method pm
  ON f.payment_method_key = pm.payment_method_key
GROUP BY pm.payment_method_name;

/* =====================================================
Query 6: Geographic Analysis
Purpose: High revenue locations
===================================================== */
SELECT
    c.state,
    SUM(f.line_total) AS total_revenue,
    COUNT(DISTINCT c.customer_key) AS total_customers,
    SUM(f.line_total) / COUNT(DISTINCT c.customer_key) AS avg_revenue_per_customer
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
  ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.state;

/* =====================================================
Query 7: Customer Lifetime Value
Purpose: Analyze customer value
===================================================== */
SELECT
    c.customer_id,
    c.full_name,
    SUM(f.line_total) AS total_spent,
    COUNT(DISTINCT f.transaction_id) AS transaction_count,
    CURRENT_DATE - c.registration_date AS days_since_registration,
    AVG(f.line_total) AS avg_order_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
  ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.full_name, c.registration_date;

/* =====================================================
Query 8: Product Profitability Analysis
Purpose: Identify most profitable products
===================================================== */
SELECT
    p.product_name,
    p.category,
    SUM(f.profit) AS total_profit,
    (SUM(f.profit) / NULLIF(SUM(f.line_total),0)) * 100 AS profit_margin,
    SUM(f.line_total) AS revenue,
    SUM(f.quantity) AS units_sold
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
  ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_profit DESC;

/* =====================================================
Query 9: Day of Week Sales Pattern
Purpose: Temporal analysis
===================================================== */
SELECT
    d.day_name,
    AVG(f.line_total) AS avg_daily_revenue,
    AVG(f.quantity) AS avg_daily_transactions,
    SUM(f.line_total) AS total_revenue
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
  ON f.date_key = d.date_key
GROUP BY d.day_name;

/* =====================================================
Query 10: Discount Impact Analysis
Purpose: Analyze discount effectiveness
===================================================== */
SELECT
    CASE
        WHEN discount_amount = 0 THEN '0%'
        WHEN discount_amount <= 10 THEN '1-10%'
        WHEN discount_amount <= 25 THEN '11-25%'
        WHEN discount_amount <= 50 THEN '26-50%'
        ELSE '50%+'
    END AS discount_range,
    AVG(discount_amount) AS avg_discount_pct,
    SUM(quantity) AS total_quantity_sold,
    SUM(line_total) AS total_revenue,
    AVG(line_total) AS avg_line_total
FROM warehouse.fact_sales
GROUP BY discount_range;
