from datetime import datetime, timedelta

from airflow.sdk import dag, task
from finstream_failure_alerts import log_task_failure


@dag(
    dag_id="finstream_data_quality",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "on_failure_callback": log_task_failure,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["finstream", "data-quality"],
)
def finstream_data_quality():

    @task
    def check_database_connection():
        import os
        import psycopg

        with psycopg.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), COUNT(*) FROM transactions")
                database, count = cur.fetchone()

        print(f"Database: {database}")
        print(f"Transactions available: {count}")


    @task
    def validate_transactions():
        import os
        import psycopg

        checks = {
            "duplicate_event_ids": """
                SELECT COUNT(*) FROM (
                    SELECT event_id
                    FROM transactions
                    GROUP BY event_id
                    HAVING COUNT(*) > 1
                ) duplicates
            """,
            "missing_event_ids": """
                SELECT COUNT(*)
                FROM transactions
                WHERE event_id IS NULL
            """,
            "invalid_amounts": """
                SELECT COUNT(*)
                FROM transactions
                WHERE amount IS NULL OR amount <= 0
            """,
        }

        with psycopg.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                for name, query in checks.items():
                    cur.execute(query)
                    failures = cur.fetchone()[0]
                    print(f"{name}: {failures}")

                    if failures:
                        raise ValueError(
                            f"Data quality check failed: {name} = {failures}"
                        )

        print("All transaction data-quality checks passed.")


    @task
    def reconcile_transaction_metrics():
        import os
        from pathlib import Path
        import psycopg

        sql_path = Path(
            "/opt/finstream/sql/analytics/06_current_pipeline_reconciliation.sql"
        )
        query = sql_path.read_text(encoding="utf-8-sig")

        with psycopg.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                windows_checked, matched_windows, mismatched_windows = cur.fetchone()

        print(f"Windows checked: {windows_checked}")
        print(f"Matched windows: {matched_windows}")
        print(f"Mismatched windows: {mismatched_windows}")

        if windows_checked == 0:
            raise ValueError("Reconciliation failed: no metric windows found.")

        if mismatched_windows > 0:
            raise ValueError(
                f"Reconciliation failed: {mismatched_windows} mismatched windows."
            )

        print("Transaction metrics reconciliation passed.")


    @task
    def generate_transaction_report():
        import os
        from pathlib import Path
        import psycopg

        sql_path = Path(
            "/opt/finstream/sql/analytics/01_transaction_summary.sql"
        )
        query = sql_path.read_text(encoding="utf-8-sig")

        with psycopg.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [column.name for column in cur.description]
                rows = cur.fetchall()

        if not rows:
            raise ValueError("Transaction report contains no records.")

        print("FINSTREAM FINANCIAL TRANSACTION REPORT")
        print("=" * 50)

        for row in rows:
            print("-" * 50)
            for column, value in zip(columns, row):
                print(f"{column}: {value}")

        print("=" * 50)
        print(f"Report generated successfully for {len(rows)} currencies.")

    (
        check_database_connection()
        >> validate_transactions()
        >> reconcile_transaction_metrics()
        >> generate_transaction_report()
    )




finstream_data_quality()
