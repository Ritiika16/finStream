-- Financial transaction summary by currency
-- Source: PostgreSQL transactions table

SELECT
    currency,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(amount), 2) AS total_transaction_value,
    ROUND(AVG(amount), 2) AS average_transaction_value,
    ROUND(MAX(amount), 2) AS maximum_transaction_value,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS successful_transactions,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_transactions,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'SUCCESS')
        / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_percentage
FROM transactions
GROUP BY currency
ORDER BY total_transaction_value DESC;
