from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FinStream-Streaming-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "earliest")
    .load()
)

print("STREAMING_SOURCE:", stream.isStreaming)
print("STREAMING_SCHEMA:")
stream.printSchema()

spark.stop()
