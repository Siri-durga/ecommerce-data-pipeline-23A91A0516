CREATE SCHEMA IF NOT EXISTS warehouse;

-- =========================
-- DIM CUSTOMER (SCD TYPE 2)
-- =========================
CREATE TABLE warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(20),
    customer_segment VARCHAR(20),
    registration_date DATE,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- =========================
-- DIM PRODUCTS (SCD TYPE 2)
-- =========================
CREATE TABLE warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    brand VARCHAR(50),
    price_range VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- =========================
-- DIM DATE
-- =========================
CREATE TABLE warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- =========================
-- DIM PAYMENT METHOD
-- =========================
CREATE TABLE warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50),
    payment_type VARCHAR(20)
);

-- =========================
-- FACT SALES
-- =========================
CREATE TABLE warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    payment_method_key INTEGER NOT NULL,
    transaction_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    line_total DECIMAL(10,2),
    profit DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (date_key) REFERENCES warehouse.dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES warehouse.dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES warehouse.dim_products(product_key),
    FOREIGN KEY (payment_method_key) REFERENCES warehouse.dim_payment_method(payment_method_key)
);

-- INDEXES
CREATE INDEX idx_fact_date ON warehouse.fact_sales(date_key);
CREATE INDEX idx_fact_customer ON warehouse.fact_sales(customer_key);
CREATE INDEX idx_fact_product ON warehouse.fact_sales(product_key);
CREATE INDEX idx_fact_payment ON warehouse.fact_sales(payment_method_key);

-- =========================
-- AGGREGATE TABLES
-- =========================
CREATE TABLE warehouse.agg_daily_sales (
    date_key INTEGER PRIMARY KEY,
    total_transactions INTEGER,
    total_revenue DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    unique_customers INTEGER
);

CREATE TABLE warehouse.agg_product_performance (
    product_key INTEGER PRIMARY KEY,
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    avg_discount_percentage DECIMAL(5,2)
);

CREATE TABLE warehouse.agg_customer_metrics (
    customer_key INTEGER PRIMARY KEY,
    total_transactions INTEGER,
    total_spent DECIMAL(12,2),
    avg_order_value DECIMAL(12,2),
    last_purchase_date DATE
);
