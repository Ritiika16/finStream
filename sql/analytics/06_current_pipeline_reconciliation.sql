-- Reconcile current pipeline metrics against persisted transactions
-- Scope: INR windows from November 19, 2026 onward
-- Historical mismatches remain visible in 05_metrics_reconciliation.sql

WITH window_comparison AS (
    SELECT
        m.metric_id,
        m.window_start,
        m.window_end,
        m.currency,
        m.transaction_count AS stored_count,
        COUNT(t.event_id) AS actual_count,
        m.total_amount AS stored_amount,
        COALESCE(SUM(t.amount), 0) AS actual_amount
    FROM transaction_metrics m
    LEFT JOIN transactions t
        ON t.currency = m.currency
       AND t.event_timestamp >= m.window_start
       AND t.event_timestamp < m.window_end
    WHERE m.currency = 'INR'
      AND m.window_start >= '2026-11-19 00:00:00+00'
      AND m.window_end <= NOW()
    GROUP BY
        m.metric_id,
        m.window_start,
        m.window_end,
        m.currency,
        m.transaction_count,
        m.total_amount
)
SELECT
    COUNT(*) AS windows_checked,
    COUNT(*) FILTER (
        WHERE stored_count = actual_count
          AND stored_amount = actual_amount
    ) AS matched_windows,
    COUNT(*) FILTER (
        WHERE stored_count <> actual_count
           OR stored_amount <> actual_amount
    ) AS mismatched_windows
FROM window_comparison;
