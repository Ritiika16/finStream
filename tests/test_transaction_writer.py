from unittest.mock import MagicMock, patch

import pytest

from src.database.transaction_writer import (
    INSERT_TRANSACTION_SQL,
    insert_transactions,
)


def sample_transaction(event_id="test-event-001"):
    return {
        "event_id": event_id,
        "transaction_id": "test-txn-001",
        "customer_id": "CUST-TEST",
        "merchant_id": "MERCH-TEST",
        "amount": "1500.00",
        "currency": "INR",
        "payment_method": "UPI",
        "transaction_type": "PAYMENT",
        "status": "SUCCESS",
        "location": "Ahmedabad",
        "device_id": "DEV-TEST",
        "event_timestamp": "2026-11-20T04:00:00+00:00",
        "kafka_partition": 0,
        "kafka_offset": 100,
    }


def test_empty_batch_skips_database():
    with patch(
        "src.database.transaction_writer.get_connection"
    ) as mock_connection:
        assert insert_transactions([]) == 0
        mock_connection.assert_not_called()


def test_transactions_are_inserted():
    connection = MagicMock()
    cursor = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.rowcount = 2

    transactions = [
        sample_transaction("event-001"),
        sample_transaction("event-002"),
    ]

    with patch(
        "src.database.transaction_writer.get_connection",
        return_value=connection,
    ):
        result = insert_transactions(transactions)

    assert result == 2
    cursor.executemany.assert_called_once_with(
        INSERT_TRANSACTION_SQL,
        transactions,
    )


def test_duplicate_events_use_conflict_protection():
    assert "ON CONFLICT (event_id) DO NOTHING" in INSERT_TRANSACTION_SQL


def test_duplicate_batch_reports_zero_inserts():
    connection = MagicMock()
    cursor = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.rowcount = 0

    with patch(
        "src.database.transaction_writer.get_connection",
        return_value=connection,
    ):
        result = insert_transactions([sample_transaction()])

    assert result == 0


def test_database_error_propagates():
    connection = MagicMock()
    cursor = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.executemany.side_effect = RuntimeError("Database unavailable")

    with patch(
        "src.database.transaction_writer.get_connection",
        return_value=connection,
    ):
        with pytest.raises(RuntimeError, match="Database unavailable"):
            insert_transactions([sample_transaction()])
