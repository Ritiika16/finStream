from pyspark.sql import SparkSession
from spark.processing.transaction_parser import parse_transactions
from spark.processing.transaction_validator import validate_transactions
from spark.processing.rejected_event_router import (
    split_transactions,
    prepare_rejected_events
)
from spark.processing.rejected_event_publisher import publish_rejected_events

spark = (
    SparkSession.builder
    .appName("FinStream-Transaction-Validation-Stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "latest")
    .load()
)

validated_stream = validate_transactions(
    parse_transactions(kafka_stream)
)

def process_batch(batch_df, batch_id):
    valid_df, invalid_df = split_transactions(batch_df)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    print(
        f"BATCH_ID={batch_id} "
        f"VALID={valid_count} "
        f"INVALID={invalid_count}",
        flush=True
    )

    if invalid_count > 0:
        publish_rejected_events(
            prepare_rejected_events(invalid_df)
        )

query = (
    validated_stream.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/opt/finstream/checkpoints/transaction_validation"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
