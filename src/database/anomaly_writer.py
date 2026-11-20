from src.database.connection import get_connection


INSERT_ALERT_SQL = """
INSERT INTO anomaly_alerts (
    event_id,
    customer_id,
    anomaly_type,
    severity,
    transaction_count,
    amount,
    window_start,
    window_end
)
VALUES (
    %(event_id)s,
    %(customer_id)s,
    %(anomaly_type)s,
    %(severity)s,
    %(transaction_count)s,
    %(amount)s,
    %(window_start)s,
    %(window_end)s
)
ON CONFLICT ON CONSTRAINT uq_anomaly_alert
DO NOTHING
"""


def insert_anomaly_alerts(alerts):
    """Insert anomaly alerts atomically without creating duplicates."""

    if not alerts:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                INSERT_ALERT_SQL,
                alerts,
            )
            inserted_count = cursor.rowcount

    return inserted_count
