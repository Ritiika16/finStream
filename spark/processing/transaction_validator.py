from pyspark.sql.functions import (
    col, lit, when, trim, length, array, array_remove, concat_ws
)


def validate_transactions(df):
    def missing(field):
        return col(field).isNull() | (length(trim(col(field))) == 0)

    checks = [
        (col("raw_json").isNull() | col("event_id").isNull(), "MALFORMED_OR_MISSING_JSON"),
        (missing("schema_version"), "MISSING_SCHEMA_VERSION"),
        (missing("event_id"), "MISSING_EVENT_ID"),
        (missing("transaction_id"), "MISSING_TRANSACTION_ID"),
        (missing("customer_id"), "MISSING_CUSTOMER_ID"),
        (missing("merchant_id"), "MISSING_MERCHANT_ID"),
        (col("amount").isNull() | (col("amount") <= 0), "INVALID_AMOUNT"),
        (col("currency").isNull() | (~col("currency").isin("INR")), "INVALID_CURRENCY"),
        (
            col("status").isNull() |
            (~col("status").isin("SUCCESS", "FAILED", "PENDING")),
            "INVALID_STATUS"
        ),
        (col("event_timestamp").isNull(), "INVALID_TIMESTAMP"),
    ]

    reasons = array(*[
        when(condition, lit(reason)).otherwise(lit(None).cast("string"))
        for condition, reason in checks
    ])

    df = df.withColumn(
        "rejection_reason",
        concat_ws(",", reasons)
    )

    return df.withColumn(
        "validation_status",
        when(
            length(col("rejection_reason")) > 0,
            lit("INVALID")
        ).otherwise(lit("VALID"))
    )
