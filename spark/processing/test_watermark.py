from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from spark.processing.event_time_processor import process_event_time
import tempfile
import os
import json

spark = (
    SparkSession.builder
    .appName("FinStream-Watermark-Test")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

def write_events(folder, filename, records):
    with open(os.path.join(folder, filename), "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

with tempfile.TemporaryDirectory() as input_dir:
    stream_df = (
        spark.readStream
        .schema("event_id STRING, event_timestamp STRING")
        .json(input_dir)
    )

    processed_df = process_event_time(stream_df)

    query = (
        processed_df.writeStream
        .format("memory")
        .queryName("finstream_watermark_test")
        .outputMode("append")
        .start()
    )

    try:
        write_events(input_dir, "batch1.json", [
            {"event_id": "EVENT-1", "event_timestamp": "2026-11-18 10:00:00"},
            {"event_id": "EVENT-2", "event_timestamp": "2026-11-18 10:05:00"}
        ])
        query.processAllAvailable()

        write_events(input_dir, "batch2.json", [
            {"event_id": "EVENT-3", "event_timestamp": "2026-11-18 10:01:00"},
            {"event_id": "EVENT-4", "event_timestamp": "2026-11-18 10:04:00"}
        ])
        query.processAllAvailable()

        result = spark.sql(
            "SELECT event_id FROM finstream_watermark_test ORDER BY event_id"
        )
        ids = [row.event_id for row in result.collect()]

        print("OUTPUT_EVENT_IDS:", ids)
        print("LATE_EVENT_DROPPED:", "EVENT-3" not in ids)
        print("ON_TIME_EVENT_RETAINED:", "EVENT-4" in ids)

        assert "EVENT-3" not in ids, "Late event was not dropped"
        assert "EVENT-4" in ids, "On-time event was incorrectly dropped"
    finally:
        query.stop()

spark.stop()
