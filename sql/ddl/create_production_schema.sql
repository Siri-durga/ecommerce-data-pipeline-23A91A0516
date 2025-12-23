-- =====================================================
-- PRODUCTION: CUSTOMERS
-- =====================================================
CREATE TABLE IF NOT EXISTS production.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20),
    registration_date DATE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- PRODUCTION: PRODUCTS
-- =====================================================
CREATE TABLE IF NOT EXISTS production.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    sub_category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) NOT NULL CHECK (cost >= 0),
    brand VARCHAR(100),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    supplier_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (cost < price)
);

-- =====================================================
-- PRODUCTION: TRANSACTIONS
-- =====================================================
CREATE TABLE IF NOT EXISTS production.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_time TIME NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    shipping_address VARCHAR(255),
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_transactions_customer
        FOREIGN KEY (customer_id)
        REFERENCES production.customers(customer_id)
);

-- =====================================================
-- PRODUCTION: TRANSACTION ITEMS
-- =====================================================
CREATE TABLE IF NOT EXISTS production.transaction_items (
    item_id VARCHAR(20) PRIMARY KEY,
    transaction_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    discount_percentage DECIMAL(5,2) CHECK (discount_percentage BETWEEN 0 AND 100),
    line_total DECIMAL(12,2) NOT NULL CHECK (line_total >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_items_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES production.transactions(transaction_id),
    CONSTRAINT fk_items_product
        FOREIGN KEY (product_id)
        REFERENCES production.products(product_id)
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_transactions_customer
    ON production.transactions(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_date
    ON production.transactions(transaction_date);

CREATE INDEX IF NOT EXISTS idx_items_transaction
    ON production.transaction_items(transaction_id);

CREATE INDEX IF NOT EXISTS idx_items_product
    ON production.transaction_items(product_id);
