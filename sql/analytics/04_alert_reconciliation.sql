-- Transaction-to-alert reconciliation
-- Checks whether transaction-level anomaly alerts reference valid transactions

SELECT
    COUNT(*) AS total_transaction_level_alerts,
    COUNT(*) FILTER (
        WHERE t.event_id IS NOT NULL
    ) AS matched_alerts,
    COUNT(*) FILTER (
        WHERE t.event_id IS NULL
    ) AS orphaned_alerts,
    COUNT(*) FILTER (
        WHERE t.event_id IS NOT NULL
          AND a.customer_id IS DISTINCT FROM t.customer_id
    ) AS customer_mismatches,
    COUNT(*) FILTER (
        WHERE t.event_id IS NOT NULL
          AND a.amount IS DISTINCT FROM t.amount
    ) AS amount_mismatches
FROM anomaly_alerts a
LEFT JOIN transactions t
    ON a.event_id = t.event_id
WHERE a.event_id IS NOT NULL;
