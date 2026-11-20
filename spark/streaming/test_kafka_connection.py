from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FinStream-Kafka-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "financial.transactions.raw")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

df.selectExpr(
    "CAST(key AS STRING) AS customer_id",
    "CAST(value AS STRING) AS transaction_json"
).show(5, truncate=False)

spark.stop()
