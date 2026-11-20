from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from spark.processing.transaction_metrics import calculate_transaction_metrics

spark = (
    SparkSession.builder
    .appName("FinStream-Window-Metrics-Test")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = [
    ("TXN-1", "2026-11-18 10:01:00", "INR", 100.0),
    ("TXN-2", "2026-11-18 10:02:00", "INR", 200.0),
    ("TXN-3", "2026-11-18 10:03:00", "INR", 300.0)
]

df = spark.createDataFrame(
    data,
    ["transaction_id", "event_time", "currency", "amount"]
).withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

metrics_df = calculate_transaction_metrics(df)

result = metrics_df.filter(
    (col("window_start") == "2026-11-18 10:00:00") &
    (col("window_end") == "2026-11-18 10:05:00")
).collect()

assert len(result) == 1, f"Expected one matching window, got {len(result)}"

row = result[0]

print("WINDOW_START:", row.window_start)
print("WINDOW_END:", row.window_end)
print("TRANSACTION_COUNT:", row.transaction_count)
print("TOTAL_AMOUNT:", row.total_amount)
print("AVERAGE_AMOUNT:", row.average_amount)
print("MAXIMUM_AMOUNT:", row.maximum_amount)

assert row.transaction_count == 3
assert row.total_amount == 600.0
assert row.average_amount == 200.0
assert row.maximum_amount == 300.0

print("WINDOW_METRICS_TEST: PASSED")

spark.stop()
