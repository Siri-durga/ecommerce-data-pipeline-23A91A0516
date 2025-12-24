-- =========================
-- COMPLETENESS CHECKS
-- =========================

-- Null checks for mandatory fields
SELECT 'customers.email' AS field, COUNT(*) AS violations
FROM production.customers
WHERE email IS NULL OR email = '';

SELECT 'products.price' AS field, COUNT(*) AS violations
FROM production.products
WHERE price IS NULL;

-- =========================
-- UNIQUENESS CHECKS
-- =========================

-- Duplicate customer emails
SELECT email, COUNT(*) AS duplicate_count
FROM production.customers
GROUP BY email
HAVING COUNT(*) > 1;

-- =========================
-- REFERENTIAL INTEGRITY
-- =========================

-- Orphan transactions (no customer)
SELECT COUNT(*) AS orphan_transactions
FROM production.transactions t
LEFT JOIN production.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (missing transaction)
SELECT COUNT(*) AS orphan_items_transaction
FROM production.transaction_items ti
LEFT JOIN production.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (missing product)
SELECT COUNT(*) AS orphan_items_product
FROM production.transaction_items ti
LEFT JOIN production.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- =========================
-- CONSISTENCY CHECKS
-- =========================

-- line_total mismatch
SELECT COUNT(*) AS mismatches
FROM production.transaction_items
WHERE ABS(
    line_total -
    (quantity * unit_price * (1 - discount_percentage / 100.0))
) > 0.01;

-- transaction total mismatch
SELECT COUNT(*) AS mismatches
FROM production.transactions t
JOIN (
    SELECT transaction_id, SUM(line_total) AS calc_total
    FROM production.transaction_items
    GROUP BY transaction_id
) s ON t.transaction_id = s.transaction_id
WHERE ABS(t.total_amount - s.calc_total) > 0.01;

-- =========================
-- BUSINESS RULES
-- =========================

-- cost < price
SELECT COUNT(*) AS violations
FROM production.products
WHERE cost >= price;

-- Discount range
SELECT COUNT(*) AS violations
FROM production.transaction_items
WHERE discount_percentage < 0 OR discount_percentage > 100;
