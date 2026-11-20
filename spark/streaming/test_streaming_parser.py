from pyspark.sql import SparkSession
from spark.processing.transaction_parser import parse_transactions

spark = SparkSession.builder.appName("FinStream-Streaming-Parser-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_stream = parse_transactions(kafka_stream)

print("PARSED_STREAM_IS_STREAMING:", parsed_stream.isStreaming)
print("PARSED_STREAM_SCHEMA:")
parsed_stream.printSchema()

assert parsed_stream.isStreaming
assert len(parsed_stream.columns) == 19
assert parsed_stream.schema["amount"].dataType.simpleString() == "decimal(18,2)"
assert parsed_stream.schema["event_timestamp"].dataType.simpleString() == "timestamp"

print("STREAMING_PARSER_SCHEMA_VALIDATED")

spark.stop()
