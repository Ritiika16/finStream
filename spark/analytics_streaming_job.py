from pyspark.sql import SparkSession

from spark.processing.valid_transaction_stream import (
    prepare_valid_transaction_stream
)
from spark.processing.transaction_metrics import (
    calculate_transaction_metrics
)
from spark.processing.anomaly_detector import (
    detect_high_value_transactions,
    detect_high_frequency_transactions
)

from src.database.streaming_writer import (
    persist_transactions,
    persist_high_value_alerts,
    persist_high_frequency_alerts,
    persist_metrics
)

spark = (
    SparkSession.builder
    .appName("FinStream-Real-Time-Analytics")
    .config("spark.sql.shuffle.partitions", "2")
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

valid_stream = prepare_valid_transaction_stream(kafka_stream)

metrics_stream = calculate_transaction_metrics(valid_stream)
high_value_stream = detect_high_value_transactions(valid_stream)
high_frequency_stream = detect_high_frequency_transactions(valid_stream)


def log_metrics(batch_df, batch_id):
    print(f"METRICS_BATCH_ID={batch_id}", flush=True)
    batch_df.show(20, truncate=False)


def log_high_value(batch_df, batch_id):
    print(f"HIGH_VALUE_BATCH_ID={batch_id}", flush=True)
    batch_df.show(20, truncate=False)


def log_high_frequency(batch_df, batch_id):
    print(f"HIGH_FREQUENCY_BATCH_ID={batch_id}", flush=True)
    batch_df.show(20, truncate=False)


metrics_query = (
    metrics_stream.writeStream
    .foreachBatch(persist_metrics)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/finstream/checkpoints/transaction_metrics"
    )
    .start()
)

high_value_query = (
    high_value_stream.writeStream
    .foreachBatch(persist_high_value_alerts)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/finstream/checkpoints/high_value_alerts"
    )
    .start()
)

high_frequency_query = (
    high_frequency_stream.writeStream
    .foreachBatch(persist_high_frequency_alerts)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/finstream/checkpoints/high_frequency_alerts"
    )
    .start()
)


transactions_query = (
    valid_stream.writeStream
    .foreachBatch(persist_transactions)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/finstream/checkpoints/valid_transactions_postgres"
    )
    .start()
)

spark.streams.awaitAnyTermination()
