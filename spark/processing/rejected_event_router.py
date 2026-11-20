from pyspark.sql.functions import (
    col, struct, to_json, current_timestamp
)


def split_transactions(validated_df):
    valid_df = validated_df.filter(
        col("validation_status") == "VALID"
    )

    invalid_df = validated_df.filter(
        col("validation_status") == "INVALID"
    )

    return valid_df, invalid_df


def prepare_rejected_events(invalid_df):
    return (
        invalid_df
        .select(
            col("kafka_key").cast("string").alias("key"),
            to_json(
                struct(
                    col("event_id"),
                    col("transaction_id"),
                    col("raw_json"),
                    col("rejection_reason"),
                    col("kafka_topic").alias("source_topic"),
                    col("kafka_partition").alias("source_partition"),
                    col("kafka_offset").alias("source_offset"),
                    current_timestamp().alias("rejected_at")
                )
            ).alias("value")
        )
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value"
        )
    )
