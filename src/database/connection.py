import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create a PostgreSQL connection for FinStream."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "financial_streaming"),
        user=os.getenv("POSTGRES_USER", "finstream"),
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )
