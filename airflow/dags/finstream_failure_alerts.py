import logging

logger = logging.getLogger("finstream.alerts")


def log_task_failure(context):
    """Record structured details when an Airflow task fails."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    if task_instance is None:
        logger.error("Airflow task failure callback missing task instance")
        return

    logger.error(
        "FINSTREAM_TASK_FAILURE dag_id=%s task_id=%s run_id=%s "
        "try_number=%s exception_type=%s log_url=%s",
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
        task_instance.try_number,
        type(exception).__name__ if exception else "Unknown",
        task_instance.log_url,
    )
