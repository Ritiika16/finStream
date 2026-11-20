from pyspark.sql.functions import (
    col,
    count,
    window,
    lit,
    when
)

HIGH_VALUE_THRESHOLD = 100000.0
HIGH_FREQUENCY_THRESHOLD = 5


def detect_high_value_transactions(events_df):
    """
    Flag individual transactions above INR 100,000.
    """

    return (
        events_df
        .filter(
            (col("currency") == "INR") &
            (col("amount") > HIGH_VALUE_THRESHOLD)
        )
        .withColumn("anomaly_type", lit("HIGH_VALUE_TRANSACTION"))
        .withColumn("severity", lit("HIGH"))
    )


def detect_high_frequency_transactions(events_df):
    """
    Detect customers making more than 5 transactions
    within a sliding 5-minute event-time window.
    """

    return (
        events_df
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("customer_id")
        )
        .agg(
            count("*").alias("transaction_count")
        )
        .filter(
            col("transaction_count") > HIGH_FREQUENCY_THRESHOLD
        )
        .select(
            col("customer_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("transaction_count")
        )
        .withColumn("anomaly_type", lit("HIGH_FREQUENCY_ACTIVITY"))
        .withColumn("severity", lit("HIGH"))
    )
