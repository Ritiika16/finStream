from decimal import Decimal
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from spark.processing.transaction_schema import TRANSACTION_SCHEMA
from spark.processing.transaction_validator import validate_transactions

spark = SparkSession.builder.appName("FinStream-Validation-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

records = [
    ("valid", "CUST-001", "500.00", "INR"),
    ("negative_amount", "CUST-002", "-100.00", "INR"),
    ("missing_customer", None, "250.00", "INR"),
    ("unsupported_currency", "CUST-004", "300.00", "USD"),
]

rows = [
    (
        "1.0", f"EVT-{name}", f"TXN-{name}",
        customer_id, "MERCHANT-001", Decimal(amount),
        currency, "UPI", "PAYMENT", "SUCCESS",
        "Ahmedabad", "DEVICE-001",
        datetime(2026, 11, 18, tzinfo=timezone.utc)
    )
    for name, customer_id, amount, currency in records
]

df = spark.createDataFrame(rows, schema=TRANSACTION_SCHEMA)
df = df.withColumn("raw_json", col("transaction_id"))

validated = validate_transactions(df)

validated.select(
    "transaction_id",
    "customer_id",
    "amount",
    "currency",
    "validation_status", "rejection_reason"
).show(truncate=False)

spark.stop()
