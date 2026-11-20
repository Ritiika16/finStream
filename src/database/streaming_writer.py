from uuid import UUID
from datetime import timezone

from src.database.transaction_writer import insert_transactions
from src.database.anomaly_writer import insert_anomaly_alerts
from src.database.metrics_writer import upsert_transaction_metrics


def to_utc(value):
    """Normalize Spark timestamps to UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def persist_transactions(batch_df, batch_id):
    """Persist validated transactions with duplicate protection."""

    columns = [
        "event_id", "transaction_id", "customer_id",
        "merchant_id", "amount", "currency",
        "payment_method", "transaction_type", "status",
        "location", "device_id", "event_timestamp",
        "kafka_partition", "kafka_offset"
    ]

    records = []

    for row in batch_df.select(*columns).toLocalIterator():
        record = row.asDict()
        record["event_id"] = UUID(record["event_id"])
        record["event_timestamp"] = to_utc(record["event_timestamp"])
        records.append(record)

    inserted = insert_transactions(records)

    print(
        f"TRANSACTIONS_BATCH={batch_id} "
        f"RECEIVED={len(records)} INSERTED={inserted}",
        flush=True
    )


def persist_high_value_alerts(batch_df, batch_id):
    """Persist high-value transaction alerts."""

    records = []

    for row in batch_df.toLocalIterator():
        record = row.asDict()

        records.append({
            "event_id": UUID(record["event_id"]),
            "customer_id": record["customer_id"],
            "anomaly_type": record["anomaly_type"],
            "severity": record["severity"],
            "transaction_count": 1,
            "amount": record["amount"],
            "window_start": None,
            "window_end": None
        })

    inserted = insert_anomaly_alerts(records)

    print(
        f"HIGH_VALUE_BATCH={batch_id} "
        f"RECEIVED={len(records)} INSERTED={inserted}",
        flush=True
    )


def persist_high_frequency_alerts(batch_df, batch_id):
    """Persist high-frequency activity alerts."""

    records = []

    for row in batch_df.toLocalIterator():
        record = row.asDict()

        records.append({
            "event_id": None,
            "customer_id": record["customer_id"],
            "anomaly_type": record["anomaly_type"],
            "severity": record["severity"],
            "transaction_count": record["transaction_count"],
            "amount": None,
            "window_start": to_utc(record["window_start"]),
            "window_end": to_utc(record["window_end"])
        })

    inserted = insert_anomaly_alerts(records)

    print(
        f"HIGH_FREQUENCY_BATCH={batch_id} "
        f"RECEIVED={len(records)} INSERTED={inserted}",
        flush=True
    )


def persist_metrics(batch_df, batch_id):
    """Upsert event-time windowed transaction metrics."""

    records = []

    for row in batch_df.toLocalIterator():
        record = row.asDict()

        records.append({
            "window_start": to_utc(record["window_start"]),
            "window_end": to_utc(record["window_end"]),
            "currency": record["currency"],
            "transaction_count": record["transaction_count"],
            "total_amount": record["total_amount"],
            "average_amount": record["average_amount"],
            "maximum_amount": record["maximum_amount"]
        })

    affected = upsert_transaction_metrics(records)

    print(
        f"METRICS_BATCH={batch_id} "
        f"RECEIVED={len(records)} UPSERTED={affected}",
        flush=True
    )
