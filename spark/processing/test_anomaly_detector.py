from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from spark.processing.anomaly_detector import (
    detect_high_value_transactions,
    detect_high_frequency_transactions
)

spark = (
    SparkSession.builder
    .appName("FinStream-Anomaly-Detection-Test")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = [
    ("TXN-1", "CUST-001", "2026-11-18 10:00:00", "INR", 150000.0),
    ("TXN-2", "CUST-001", "2026-11-18 10:00:30", "INR", 1000.0),
    ("TXN-3", "CUST-001", "2026-11-18 10:01:00", "INR", 2000.0),
    ("TXN-4", "CUST-001", "2026-11-18 10:01:30", "INR", 3000.0),
    ("TXN-5", "CUST-001", "2026-11-18 10:02:00", "INR", 4000.0),
    ("TXN-6", "CUST-001", "2026-11-18 10:02:30", "INR", 5000.0),
    ("TXN-7", "CUST-002", "2026-11-18 10:03:00", "INR", 500.0)
]

df = spark.createDataFrame(
    data,
    ["transaction_id", "customer_id", "event_time", "currency", "amount"]
).withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

high_value = detect_high_value_transactions(df)

high_value_ids = [
    row.transaction_id
    for row in high_value.select("transaction_id").collect()
]

print("HIGH_VALUE_TRANSACTION_IDS:", high_value_ids)

assert high_value_ids == ["TXN-1"], (
    f"Expected only TXN-1, got {high_value_ids}"
)

high_frequency = detect_high_frequency_transactions(df)

matching_windows = high_frequency.filter(
    (col("customer_id") == "CUST-001") &
    (col("window_start") == "2026-11-18 10:00:00") &
    (col("window_end") == "2026-11-18 10:05:00")
).collect()

assert len(matching_windows) == 1, (
    f"Expected one matching window, got {len(matching_windows)}"
)

print(
    "HIGH_FREQUENCY_CUSTOMER:",
    matching_windows[0].customer_id
)
print(
    "TRANSACTIONS_IN_WINDOW:",
    matching_windows[0].transaction_count
)

assert matching_windows[0].transaction_count == 6

print("ANOMALY_DETECTION_TEST: PASSED")

spark.stop()
