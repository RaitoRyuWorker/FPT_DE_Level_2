-- Customer Revenue Table Creation and Population
-- This script creates a new table called customer_revenue with total transaction amount per customer

-- Step 1: Create the customer_revenue table
CREATE TABLE IF NOT EXISTS customer_revenue (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    total_transaction_amount REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

-- Step 2: Calculate and insert aggregated data
INSERT OR REPLACE INTO customer_revenue (
    customer_id,
    customer_name,
    customer_email,
    total_transaction_amount
)
SELECT 
    c.customer_id,
    c.name as customer_name,
    c.email as customer_email,
    COALESCE(SUM(t.amount), 0.0) as total_transaction_amount
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_transaction_amount DESC;

-- Step 3: Verify the data was inserted
SELECT COUNT(*) as total_customer_revenue_records FROM customer_revenue;
