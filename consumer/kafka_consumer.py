import json
import os
import logging
import argparse

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

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


def create_consumer(group_id):
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "allow.auto.create.topics": False
    })


def run_consumer(group_id, max_messages=None):
    consumer = create_consumer(group_id)
    processed = 0

    consumer.subscribe([KAFKA_TOPIC])

    logger.info(
        "Consumer started | group=%s | topic=%s",
        group_id,
        KAFKA_TOPIC
    )

    try:
        while max_messages is None or processed < max_messages:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                transaction = json.loads(
                    message.value().decode("utf-8")
                )

                logger.info(
                    "Received | customer=%s | amount=%s | "
                    "partition=%s | offset=%s",
                    transaction.get("customer_id"),
                    transaction.get("amount"),
                    message.partition(),
                    message.offset()
                )

                # Commit only after successful processing.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                processed += 1

            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(
                    "Invalid message | partition=%s | offset=%s | error=%s",
                    message.partition(),
                    message.offset(),
                    exc
                )
                # Leave the offset uncommitted for investigation.
                raise

    except KeyboardInterrupt:
        logger.info("Consumer interrupted")

    finally:
        consumer.close()
        logger.info("Consumer stopped | processed=%s", processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--group",
        default="finstream-python-consumer",
        help="Kafka consumer group ID"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Maximum messages to consume"
    )

    args = parser.parse_args()

    if args.count is not None and args.count < 0:
        parser.error("--count cannot be negative")

    run_consumer(args.group, args.count)