from pyspark.sql.functions import col, from_json
from spark.processing.transaction_schema import TRANSACTION_SCHEMA


def parse_transactions(kafka_df):
    return (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "parsed",
            from_json(col("raw_json"), TRANSACTION_SCHEMA)
        )
        .select(
            "kafka_key",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "parsed.*",
        )
    )
