from src.database.connection import get_connection


UPSERT_METRICS_SQL = """
INSERT INTO transaction_metrics (
    window_start,
    window_end,
    currency,
    transaction_count,
    total_amount,
    average_amount,
    maximum_amount
)
VALUES (
    %(window_start)s,
    %(window_end)s,
    %(currency)s,
    %(transaction_count)s,
    %(total_amount)s,
    %(average_amount)s,
    %(maximum_amount)s
)
ON CONFLICT (window_start, window_end, currency)
DO UPDATE SET
    transaction_count = EXCLUDED.transaction_count,
    total_amount = EXCLUDED.total_amount,
    average_amount = EXCLUDED.average_amount,
    maximum_amount = EXCLUDED.maximum_amount,
    updated_at = NOW()
"""


def upsert_transaction_metrics(metrics):
    """Insert or update windowed metrics atomically."""

    if not metrics:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                UPSERT_METRICS_SQL,
                metrics,
            )
            affected_count = cursor.rowcount

    return affected_count
