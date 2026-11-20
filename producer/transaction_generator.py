import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from faker import Faker

fake = Faker("en_IN")

PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING"]
TRANSACTION_TYPES = ["PAYMENT", "REFUND", "TRANSFER"]
STATUSES = ["SUCCESS", "FAILED", "PENDING"]
CURRENCIES = ["INR"]


def generate_transaction():
    amount = Decimal(str(round(random.uniform(100, 50000), 2)))

    transaction = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1, 1000):05d}",
        "merchant_id": f"MERCH-{random.randint(1, 200):04d}",
        "amount": str(amount),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "status": random.choices(
            STATUSES,
            weights=[85, 10, 5],
            k=1
        )[0],
        "location": fake.city(),
        "device_id": f"DEV-{random.randint(1, 500):05d}",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }

    return transaction

def generate_anomaly(scenario="high_value", customer_id=None):
    transaction = generate_transaction()

    if customer_id is not None:
        transaction["customer_id"] = customer_id

    if scenario == "high_value":
        transaction["amount"] = str(
            Decimal(str(random.randint(100001, 500000)))
        )
        transaction["transaction_type"] = "PAYMENT"
        transaction["status"] = "SUCCESS"

    elif scenario == "failed_payment":
        transaction["status"] = "FAILED"
        transaction["transaction_type"] = "PAYMENT"

    elif scenario == "velocity":
        transaction["transaction_type"] = "PAYMENT"
        transaction["status"] = "SUCCESS"

    else:
        raise ValueError(f"Unknown anomaly scenario: {scenario}")

    return transaction

if __name__ == "__main__":
    import json

    transaction = generate_transaction()
    print(json.dumps(transaction, indent=2))