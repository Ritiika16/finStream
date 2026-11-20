from psycopg.rows import dict_row

from src.database.connection import get_connection


INSERT_TRANSACTION_SQL = """
INSERT INTO transactions (
    event_id,
    transaction_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    payment_method,
    transaction_type,
    status,
    location,
    device_id,
    event_timestamp,
    kafka_partition,
    kafka_offset
)
VALUES (
    %(event_id)s,
    %(transaction_id)s,
    %(customer_id)s,
    %(merchant_id)s,
    %(amount)s,
    %(currency)s,
    %(payment_method)s,
    %(transaction_type)s,
    %(status)s,
    %(location)s,
    %(device_id)s,
    %(event_timestamp)s,
    %(kafka_partition)s,
    %(kafka_offset)s
)
ON CONFLICT (event_id) DO NOTHING
"""


def insert_transactions(transactions):
    """Insert transactions atomically without duplicating event IDs."""

    if not transactions:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                INSERT_TRANSACTION_SQL,
                transactions,
            )

            inserted_count = cursor.rowcount

    return inserted_count
