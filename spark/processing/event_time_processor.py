from pyspark.sql.functions import col, to_timestamp, trim


def process_event_time(valid_df):
    """
    Prepare valid financial transactions for event-time processing.

    Watermark: 2 minutes
    Deduplication key: event_id
    """

    event_df = valid_df.withColumn(
        "event_time",
        to_timestamp(col("event_timestamp"))
    )

    event_df = event_df.filter(
        col("event_time").isNotNull()
        & col("event_id").isNotNull()
        & (trim(col("event_id")) != "")
    )

    return (
        event_df
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["event_id"])
    )
