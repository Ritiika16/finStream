from pyspark.sql.functions import col


def publish_rejected_events(rejected_df):
    (
        rejected_df
        .select(
            col("key").cast("string"),
            col("value").cast("string")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("topic", "financial.transactions.invalid")
        .save()
    )
