from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from spark.processing.transaction_parser import parse_transactions
from spark.processing.transaction_validator import validate_transactions
from spark.processing.rejected_event_router import prepare_rejected_events
from spark.processing.rejected_event_publisher import publish_rejected_events

spark = SparkSession.builder.appName("FinStream-Malformed-Rejection-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

validated_df = validate_transactions(parse_transactions(kafka_df))

malformed_df = validated_df.filter(
    (col("kafka_key") == "CUST-MALFORMED-TEST") &
    (col("validation_status") == "INVALID") &
    col("rejection_reason").contains("MALFORMED_OR_MISSING_JSON")
)

count = malformed_df.count()
print("MALFORMED_EVENTS_FOUND:", count)

assert count == 1, f"Expected exactly 1 malformed test event, found {count}"

publish_rejected_events(prepare_rejected_events(malformed_df))
print("MALFORMED_EVENT_PUBLISHED_TO_INVALID_TOPIC")

spark.stop()
