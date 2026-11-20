from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from producer.transaction_generator import (
    CURRENCIES,
    PAYMENT_METHODS,
    STATUSES,
    TRANSACTION_TYPES,
    generate_anomaly,
    generate_transaction,
)


REQUIRED_FIELDS = {
    "schema_version", "event_id", "transaction_id",
    "customer_id", "merchant_id", "amount", "currency",
    "payment_method", "transaction_type", "status",
    "location", "device_id", "event_timestamp",
}


def test_transaction_has_required_fields():
    transaction = generate_transaction()
    assert set(transaction) == REQUIRED_FIELDS
    assert transaction["schema_version"] == "1.0"


def test_transaction_has_valid_identifiers():
    transaction = generate_transaction()
    UUID(transaction["event_id"])
    UUID(transaction["transaction_id"])
    assert transaction["event_id"] != transaction["transaction_id"]
    assert transaction["customer_id"].startswith("CUST-")
    assert transaction["merchant_id"].startswith("MERCH-")


def test_transaction_amount_is_valid():
    for _ in range(100):
        amount = Decimal(generate_transaction()["amount"])
        assert Decimal("100") <= amount <= Decimal("50000")
        assert amount.as_tuple().exponent >= -2


def test_transaction_uses_supported_values():
    for _ in range(100):
        transaction = generate_transaction()
        assert transaction["currency"] in CURRENCIES
        assert transaction["payment_method"] in PAYMENT_METHODS
        assert transaction["transaction_type"] in TRANSACTION_TYPES
        assert transaction["status"] in STATUSES


def test_transaction_timestamp_is_utc():
    timestamp = datetime.fromisoformat(
        generate_transaction()["event_timestamp"]
    )
    assert timestamp.tzinfo is not None
    assert timestamp.utcoffset().total_seconds() == 0


def test_high_value_anomaly():
    transaction = generate_anomaly("high_value", "CUST-TEST")
    assert Decimal(transaction["amount"]) > Decimal("100000")
    assert transaction["status"] == "SUCCESS"
    assert transaction["transaction_type"] == "PAYMENT"
    assert transaction["customer_id"] == "CUST-TEST"


def test_failed_payment_anomaly():
    transaction = generate_anomaly("failed_payment")
    assert transaction["status"] == "FAILED"
    assert transaction["transaction_type"] == "PAYMENT"


def test_velocity_anomaly():
    transaction = generate_anomaly("velocity")
    assert transaction["status"] == "SUCCESS"
    assert transaction["transaction_type"] == "PAYMENT"


def test_unknown_anomaly_scenario():
    with pytest.raises(ValueError, match="Unknown anomaly scenario"):
        generate_anomaly("invalid_scenario")


def test_event_ids_are_unique():
    event_ids = {
        generate_transaction()["event_id"]
        for _ in range(100)
    }
    assert len(event_ids) == 100
