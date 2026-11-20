from pyspark.sql import SparkSession
from spark.processing.transaction_parser import parse_transactions

spark = SparkSession.builder.appName("FinStream-JSON-Parsing-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed_df = parse_transactions(kafka_df)

print("PARSED_TRANSACTION_SCHEMA:")
parsed_df.printSchema()

print("SAMPLE_PARSED_TRANSACTIONS:")
parsed_df.select(
    "transaction_id",
    "customer_id",
    "amount",
    "currency",
    "status",
    "event_timestamp"
).show(5, truncate=False)

spark.stop()
