#!/usr/bin/env bash
set -euo pipefail

cd /opt/finstream
export PYTHONPATH=/opt/finstream

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  --conf spark.driver.memory=768m \
  --conf spark.executor.memory=512m \
  --conf spark.sql.shuffle.partitions=2 \
  spark/analytics_streaming_job.py
