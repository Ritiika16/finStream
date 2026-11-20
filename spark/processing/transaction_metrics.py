from pyspark.sql.functions import (
    col,
    window,
    count,
    sum as spark_sum,
    avg,
    max as spark_max
)

def calculate_transaction_metrics(events_df):
    """
    Calculate transaction metrics using 5-minute
    event-time windows, sliding every 1 minute.
    """

    return (
        events_df
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("currency")
        )
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount"),
            spark_max("amount").alias("maximum_amount")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("currency"),
            col("transaction_count"),
            col("total_amount"),
            col("average_amount"),
            col("maximum_amount")
        )
    )
