import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType
)

from spark.processing.transaction_validator import validate_transactions


SCHEMA = StructType([
    StructField("raw_json", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FinStream-Validator-Tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_transaction(**overrides):
    from decimal import Decimal

    transaction = {
        "raw_json": '{"event_id":"test"}',
        "schema_version": "1.0",
        "event_id": "event-001",
        "transaction_id": "txn-001",
        "customer_id": "CUST-001",
        "merchant_id": "MERCH-001",
        "amount": Decimal("1500.00"),
        "currency": "INR",
        "status": "SUCCESS",
        "event_timestamp": "2026-11-20T04:00:00+00:00",
    }
    transaction.update(overrides)
    return transaction


def validate_one(spark, **overrides):
    transaction = make_transaction(**overrides)
    df = spark.createDataFrame([transaction], schema=SCHEMA)
    return validate_transactions(df).first()


def test_valid_transaction(spark):
    result = validate_one(spark)
    assert result["validation_status"] == "VALID"
    assert result["rejection_reason"] == ""


@pytest.mark.parametrize("field,reason", [
    ("schema_version", "MISSING_SCHEMA_VERSION"),
    ("event_id", "MISSING_EVENT_ID"),
    ("transaction_id", "MISSING_TRANSACTION_ID"),
    ("customer_id", "MISSING_CUSTOMER_ID"),
    ("merchant_id", "MISSING_MERCHANT_ID"),
])
def test_missing_required_fields(spark, field, reason):
    result = validate_one(spark, **{field: None})
    assert result["validation_status"] == "INVALID"
    assert reason in result["rejection_reason"]


@pytest.mark.parametrize("amount", ["0.00", "-100.00"])
def test_invalid_amount(spark, amount):
    from decimal import Decimal

    result = validate_one(spark, amount=Decimal(amount))
    assert result["validation_status"] == "INVALID"
    assert "INVALID_AMOUNT" in result["rejection_reason"]


def test_invalid_currency(spark):
    result = validate_one(spark, currency="USD")
    assert result["validation_status"] == "INVALID"
    assert "INVALID_CURRENCY" in result["rejection_reason"]


def test_invalid_status(spark):
    result = validate_one(spark, status="UNKNOWN")
    assert result["validation_status"] == "INVALID"
    assert "INVALID_STATUS" in result["rejection_reason"]


def test_missing_timestamp(spark):
    result = validate_one(spark, event_timestamp=None)
    assert result["validation_status"] == "INVALID"
    assert "INVALID_TIMESTAMP" in result["rejection_reason"]


def test_multiple_rejection_reasons(spark):
    from decimal import Decimal

    result = validate_one(
        spark,
        customer_id=None,
        amount=Decimal("-50.00"),
        currency="USD",
    )
    assert result["validation_status"] == "INVALID"
    assert "MISSING_CUSTOMER_ID" in result["rejection_reason"]
    assert "INVALID_AMOUNT" in result["rejection_reason"]
    assert "INVALID_CURRENCY" in result["rejection_reason"]


def test_missing_json(spark):
    result = validate_one(spark, raw_json=None)
    assert result["validation_status"] == "INVALID"
    assert "MALFORMED_OR_MISSING_JSON" in result["rejection_reason"]
