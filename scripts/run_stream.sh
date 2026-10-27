#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Ignored signals stay ignored across exec, and the JVM leaves already-ignored
# shutdown signals alone. So Ctrl+C / SIGTERM to the process group only reach
# read_stream.py, which installs its own handlers and stops the queries cleanly.
trap '' INT TERM

exec spark-submit \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_KAFKA_VERSION:-4.1.2}" \
  spark-streaming/read_stream.py
