from spark.processing.transaction_parser import parse_transactions
from spark.processing.transaction_validator import validate_transactions
from spark.processing.rejected_event_router import split_transactions
from spark.processing.event_time_processor import process_event_time


def prepare_valid_transaction_stream(kafka_stream):
    """
    Parse and validate Kafka transactions, then apply
    event-time watermarking and deduplication.

    Invalid-event publishing remains the responsibility
    of the existing validation streaming job.
    """

    parsed_df = parse_transactions(kafka_stream)
    validated_df = validate_transactions(parsed_df)

    valid_df, _ = split_transactions(validated_df)

    return process_event_time(valid_df)
