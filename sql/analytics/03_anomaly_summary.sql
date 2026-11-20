-- Anomaly alert analytics
-- Summarizes detected anomalies by type and severity

SELECT
    anomaly_type,
    severity,
    COUNT(*) AS total_alerts,
    COUNT(DISTINCT customer_id) AS affected_customers,
    COUNT(DISTINCT event_id) AS transaction_level_alerts,
    COUNT(*) FILTER (
        WHERE window_start IS NOT NULL
    ) AS window_based_alerts,
    MAX(transaction_count) AS maximum_window_transaction_count,
    ROUND(MAX(amount), 2) AS maximum_flagged_amount,
    MIN(detected_at) AS first_detected_at,
    MAX(detected_at) AS latest_detected_at
FROM anomaly_alerts
GROUP BY anomaly_type, severity
ORDER BY total_alerts DESC, anomaly_type;
