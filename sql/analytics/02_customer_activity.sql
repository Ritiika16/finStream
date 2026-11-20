-- Customer transaction activity analysis
-- Identifies transaction frequency, spending, and high-value activity

SELECT
    customer_id,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS average_amount,
    COUNT(*) FILTER (
        WHERE amount > 100000 AND currency = 'INR'
    ) AS high_value_transactions,
    COUNT(*) FILTER (
        WHERE status = 'FAILED'
    ) AS failed_transactions,
    MIN(event_timestamp) AS first_transaction_at,
    MAX(event_timestamp) AS latest_transaction_at
FROM transactions
GROUP BY customer_id
ORDER BY transaction_count DESC, total_amount DESC
LIMIT 10;
