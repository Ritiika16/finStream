import json
import os
import time
import logging
import argparse

from confluent_kafka import Producer
from dotenv import load_dotenv

from producer.transaction_generator import generate_transaction, generate_anomaly

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_RAW_TOPIC", "financial.transactions.raw"
)


def delivery_callback(error, message):
    if error:
        logger.error("Delivery failed: %s", error)
    else:
        logger.info(
            "Delivered | topic=%s | partition=%s | offset=%s",
            message.topic(),
            message.partition(),
            message.offset()
        )


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "finstream-python-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "compression.type": "snappy"
    })


def publish_transaction(producer, transaction):
    payload = json.dumps(transaction).encode("utf-8")
    customer_id = transaction["customer_id"]

    producer.produce(
        topic=KAFKA_TOPIC,
        key=customer_id.encode("utf-8"),
        value=payload,
        callback=delivery_callback
    )

    producer.poll(0)

def run_producer(rate, count):
    producer = create_producer()
    delay = 1.0 / rate
    sent = 0

    logger.info("Starting FinStream Kafka producer")

    try:
        while count is None or sent < count:
            # Inject anomaly scenarios periodically
            if sent % 50 == 0 and sent > 0:
                transaction = generate_anomaly("high_value")

            elif sent % 30 == 0 and sent > 0:
                transaction = generate_anomaly(
                    "failed_payment",
                    customer_id="CUST-FRAUD-001"
                )

            elif sent % 20 == 0 and sent > 0:
                transaction = generate_anomaly(
                    "velocity",
                    customer_id="CUST-VELOCITY-001"
                )

            else:
                transaction = generate_transaction()

            publish_transaction(producer, transaction)
            sent += 1

            time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Producer interrupted")

    finally:
        remaining = producer.flush(timeout=15)

        if remaining:
            logger.error("%s messages remain undelivered", remaining)

        logger.info("Producer stopped | submitted=%s", sent)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rate",
        type=float,
        default=5,
        help="Transactions per second"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of transactions to produce"
    )

    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate must be greater than zero")

    if args.count is not None and args.count < 0:
        parser.error("--count cannot be negative")

    run_producer(args.rate, args.count)