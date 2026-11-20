from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)

TRANSACTION_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("location", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
])
