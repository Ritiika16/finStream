# FinStream - Real-Time Financial Transaction Streaming Pipeline

An end-to-end, event-driven data engineering project that ingests synthetic financial transactions through Apache Kafka, processes them with Apache Spark Structured Streaming, persists analytical results in PostgreSQL, and orchestrates data quality checks using Apache Airflow.

## Architecture

```text
Synthetic Transaction Generator
            |
            v
      Kafka Producer
            |
            v
financial.transactions.raw
            |
            v
  Spark Structured Streaming
            |
            +--> Schema Validation
            |       |
            |       +--> Invalid Event Routing
            |
            +--> Event-Time Processing
            |       |
            |       +--> Watermarking & Deduplication
            |
            +--> Windowed Transaction Metrics
            |
            +--> Anomaly Detection
            |       |
            |       +--> High-Value Alerts
            |       +--> High-Frequency Alerts
            |
            v
       PostgreSQL
            |
            +--> Transactions
            +--> Anomaly Alerts
            +--> Transaction Metrics
            |
            v
       Apache Airflow
            |
            +--> Data Quality Checks
            +--> Metric Reconciliation
            +--> Reporting
            +--> Failure Logging
```
## Technology Stack

| Component | Technology |
|---|---|
| Language | Python, SQL |
| Event Streaming | Apache Kafka 3.9.1 |
| Stream Processing | Apache Spark 3.5.9, PySpark |
| Database | PostgreSQL 16 |
| Orchestration | Apache Airflow 3.1.3 |
| Infrastructure | Docker, Docker Compose |
| Testing | pytest |
| Version Control | Git, GitHub |

## Key Features

- Synthetic financial transaction generation with configurable publishing rates.
- Kafka-based ingestion using partitioned topics.
- Continuous processing with Spark Structured Streaming.
- Transaction schema validation and invalid-event routing.
- Event-time processing, watermarking, and deduplication.
- Window-based transaction metrics and anomaly detection.
- High-value and high-frequency transaction alerts.
- Idempotent PostgreSQL transaction persistence.
- SQL analytics and reconciliation.
- Airflow-based data quality checks and reporting.
- Spark checkpointing and restart recovery.
- Docker health checks, dependencies, and restart policies.

## Kafka Topics

| Topic | Purpose |
|---|---|
| financial.transactions.raw | Incoming transaction events |
| financial.transactions.invalid | Invalid or rejected events |
| financial.transactions.alerts | Anomaly alerts |
| financial.transactions.benchmark | Isolated producer benchmarking |

## PostgreSQL Data Model

The pipeline maintains three primary tables:

- transactions: Validated transactions with event timestamps and Kafka partition/offset metadata.
- anomaly_alerts: High-value and high-frequency anomaly records.
- transaction_metrics: Window-based transaction aggregations.

Transaction persistence uses unique event identifiers and conflict handling to prevent duplicate records during replay.

## Apache Airflow

The finstream_data_quality DAG runs every 15 minutes and performs:

1. PostgreSQL connectivity verification.
2. Transaction data quality validation.
3. Transaction metric reconciliation.
4. Data quality report generation.

The workflow includes retries, execution timeouts, and structured task failure logging.

## Automated Testing

The pytest suite covers transaction generation, transaction validation, and PostgreSQL writing with mocked dependencies.

Latest verified result: **28 tests passed**.

Run the tests:

    python -m pytest tests/ -q --tb=short

## Local Performance Benchmarks

| Benchmark | Observed Result |
|---|---|
| Synthetic generation | 10,000 transactions in 0.959 seconds |
| Generation throughput | Approximately 10,424 events/second |
| Kafka acknowledged publishing | 1,000 messages in 0.770 seconds |
| Kafka producer throughput | Approximately 1,299 messages/second |
| PostgreSQL aggregation execution | 1.403 ms |
| Indexed PostgreSQL event lookup | 0.397 ms |

These are local component benchmarks, not end-to-end throughput or production performance guarantees.

## Local Setup

### Prerequisites

- Docker Desktop with Docker Compose
- Python and pip
- Git

### Environment Configuration

Create a .env file in the project root and configure POSTGRES_PASSWORD and other required environment variables.

Never commit .env or real credentials.

### Start the Infrastructure

    docker compose up -d --build

Check container status:

    docker compose ps

### Publish Sample Transactions

    python -m producer.kafka_producer --rate 1 --count 10

The Spark container automatically launches the streaming application using docker/spark/start-streaming.sh.

### Monitor Spark

    docker compose logs -f spark

The Airflow API/UI service is exposed locally on port 8080, subject to the configured authentication setup.

### Stop the Infrastructure

    docker compose down

Avoid docker compose down -v unless intentionally deleting persistent Kafka and PostgreSQL volumes.

## Reliability and Recovery

- Kafka partition-based ingestion.
- Spark checkpoint-based offset and state recovery.
- Duplicate and replay handling.
- Late-event processing with watermarks.
- Idempotent PostgreSQL transaction writes.
- Airflow retries, timeouts, and failure callbacks.
- Docker restart policies and dependency health checks.

## Project Structure

    airflow/       Airflow DAGs and failure callbacks
    consumer/      Kafka consumer
    docker/        Custom Spark and Airflow Docker images
    producer/      Synthetic transaction generator and Kafka producer
    spark/         Streaming jobs, validation, metrics, and anomaly detection
    sql/           PostgreSQL schemas and analytical queries
    src/database/  Database connections and streaming writers
    tests/         Automated unit tests
    checkpoints/   Local Spark streaming state (Git-ignored)

## Scope and Limitations

FinStream is a local portfolio project demonstrating streaming data engineering patterns.

Its development environment uses a single Kafka broker and local Spark execution. Production deployment would require additional work around distributed scaling, high availability, security, observability, and performance testing.

## Project Highlights

- Built a Dockerized real-time financial transaction pipeline using Apache Kafka, PySpark Structured Streaming, PostgreSQL, and Apache Airflow.
- Implemented event-time processing, watermarking, duplicate handling, windowed aggregations, and anomaly detection.
- Designed idempotent PostgreSQL persistence, analytical reconciliation queries, and automated data quality orchestration.
- Validated core components with 28 passing tests and benchmarked local transaction generation, Kafka publishing, and PostgreSQL queries.
