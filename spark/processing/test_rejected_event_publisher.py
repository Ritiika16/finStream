from pyspark.sql import SparkSession
from spark.processing.transaction_parser import parse_transactions
from spark.processing.transaction_validator import validate_transactions
from spark.processing.rejected_event_router import (
    split_transactions,
    prepare_rejected_events
)
from spark.processing.rejected_event_publisher import publish_rejected_events

spark = SparkSession.builder.appName("FinStream-Rejected-Event-Publishing-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed_df = parse_transactions(kafka_df)
validated_df = validate_transactions(parsed_df)

valid_df, invalid_df = split_transactions(validated_df)
rejected_df = prepare_rejected_events(invalid_df)

print("VALID_TRANSACTIONS:", valid_df.count())
print("INVALID_TRANSACTIONS:", invalid_df.count())

if invalid_df.count() > 0:
    publish_rejected_events(rejected_df)
    print("REJECTED_EVENTS_PUBLISHED_TO_KAFKA")
else:
    print("NO_REJECTED_EVENTS_TO_PUBLISH")

spark.stop()
