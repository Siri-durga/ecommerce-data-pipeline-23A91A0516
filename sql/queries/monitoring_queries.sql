/* =========================================
Query 1: Data Freshness Check
========================================= */
SELECT
    MAX(created_at) AS warehouse_latest
FROM warehouse.fact_sales;

/* =========================================
Query 2: Daily Volume Trend (Last 30 Days)
========================================= */
SELECT
    d.full_date,
    COUNT(f.sales_key) AS daily_transactions
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
  ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date
ORDER BY d.full_date;

/* =========================================
Query 3: Data Quality Checks
========================================= */
SELECT
    COUNT(*) FILTER (WHERE customer_key IS NULL) AS null_customers,
    COUNT(*) FILTER (WHERE product_key IS NULL) AS null_products,
    COUNT(*) FILTER (WHERE date_key IS NULL) AS null_dates
FROM warehouse.fact_sales;

/* =========================================
Query 4: Orphan Records
========================================= */
SELECT
    COUNT(*) AS orphan_records
FROM warehouse.fact_sales f
LEFT JOIN warehouse.dim_customers c
  ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

/* =========================================
Query 5: Database Statistics
========================================= */
SELECT
    COUNT(*) AS total_rows
FROM warehouse.fact_sales;
