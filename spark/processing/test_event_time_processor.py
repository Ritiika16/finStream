from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, floor
from spark.processing.event_time_processor import process_event_time
import tempfile
import os
import json

spark = (
    SparkSession.builder
    .appName("FinStream-Event-Time-Test")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

with tempfile.TemporaryDirectory() as input_dir:
    records = [
        {
            "event_id": f"EVENT-{i // 2}",
            "event_timestamp": "2026-11-18 01:00:00"
        }
        for i in range(20)
    ]

    with open(os.path.join(input_dir, "events.json"), "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    schema = "event_id STRING, event_timestamp STRING"

    input_df = (
        spark.readStream
        .schema(schema)
        .json(input_dir)
    )

    processed_df = process_event_time(input_df)

    query = (
        processed_df.writeStream
        .format("memory")
        .queryName("finstream_event_time_test")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    completed = query.awaitTermination(120)

    if not completed:
        query.stop()
        raise TimeoutError("Streaming test exceeded 120 seconds")

    result = spark.sql(
        "SELECT event_id, event_time FROM finstream_event_time_test"
    )

    total = result.count()
    distinct = result.select("event_id").distinct().count()

    print("INPUT_EVENTS:", len(records))
    print("TOTAL_OUTPUT_EVENTS:", total)
    print("DISTINCT_EVENT_IDS:", distinct)
    print("DEDUPLICATION_SUCCESS:", total == distinct == 10)

    assert total == 10, f"Expected 10 unique events, found {total}"
    assert distinct == 10, "Duplicate event IDs detected"

spark.stop()
