import time

from producer.kafka_producer import (
    create_producer,
    publish_transaction
)
from producer.transaction_generator import generate_anomaly

CUSTOMER_ID = "CUST-VELOCITY-TEST-001"
TRANSACTION_COUNT = 6

producer = create_producer()

for i in range(TRANSACTION_COUNT):
    transaction = generate_anomaly(
        "velocity",
        customer_id=CUSTOMER_ID
    )

    publish_transaction(producer, transaction)

    print(
        f"SENT={i + 1} "
        f"CUSTOMER={CUSTOMER_ID} "
        f"EVENT_ID={transaction['event_id']}",
        flush=True
    )

    time.sleep(0.2)

remaining = producer.flush(timeout=15)

assert remaining == 0, (
    f"{remaining} Kafka messages remain undelivered"
)

print("VELOCITY_TEST_EVENTS_SENT:", TRANSACTION_COUNT)
print("TEST_CUSTOMER:", CUSTOMER_ID)
