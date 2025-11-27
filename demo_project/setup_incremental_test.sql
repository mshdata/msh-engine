-- ============================================
-- INCREMENTAL TEST SETUP
-- ============================================

-- Step 1: Create the orders table
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Insert initial batch of orders
INSERT INTO orders (customer_id, order_date, amount, status, updated_at) VALUES
(1, '2024-01-01 10:00:00', 100.00, 'completed', '2024-01-01 10:00:00'),
(2, '2024-01-02 11:00:00', 150.00, 'completed', '2024-01-02 11:00:00'),
(3, '2024-01-03 12:00:00', 200.00, 'pending', '2024-01-03 12:00:00'),
(1, '2024-01-04 13:00:00', 75.00, 'completed', '2024-01-04 13:00:00'),
(4, '2024-01-05 14:00:00', 300.00, 'completed', '2024-01-05 14:00:00');

-- ============================================
-- RUN YOUR dbt MODEL FIRST, THEN RUN BELOW
-- ============================================

-- Step 3: Add new orders (run this AFTER first dbt run to test incremental)
INSERT INTO orders (customer_id, order_date, amount, status, updated_at) VALUES
(2, '2024-01-06 15:00:00', 125.00, 'completed', CURRENT_TIMESTAMP),
(5, '2024-01-07 16:00:00', 250.00, 'pending', CURRENT_TIMESTAMP),
(3, '2024-01-08 17:00:00', 180.00, 'completed', CURRENT_TIMESTAMP);

-- Step 4: Update an existing order (to test merge/upsert)
UPDATE orders 
SET status = 'cancelled', 
    amount = 0.00,
    updated_at = CURRENT_TIMESTAMP
WHERE order_id = 3;

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

-- Check what's in Postgres
SELECT * FROM orders ORDER BY order_id;

-- Count records
SELECT COUNT(*) as total_orders FROM orders;
