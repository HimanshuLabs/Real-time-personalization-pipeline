#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Ctrl+C / SIGTERM must stop the stream at any moment, including while
# spark-submit is still launching the JVM and read_stream.py doesn't exist yet.
# This script catches the signal and writes a stop file, which read_stream.py
# checks at start-up and every second. The path is new for every run (inside a
# fresh mktemp directory, so the file itself doesn't exist until a stop is
# requested) and is removed when the script exits, so a stale stop never stops
# the next run.
STOP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/project1-stream.XXXXXXXX")"
export STREAM_STOP_FILE="${STOP_DIR}/stop"
trap 'rm -rf "${STOP_DIR}"' EXIT
trap ': > "${STREAM_STOP_FILE}"' INT TERM

# Spark 4.1.x crashes when it restarts on a planned-but-uncommitted batch
# (https://github.com/apache/spark/issues/55236); repair the checkpoints first.
# A failure aborts the start (set -e).
"${PYTHON:-python3}" scripts/repair_stream_checkpoints.py

# Ignored signals stay ignored across exec, and the JVM leaves already-ignored
# shutdown signals alone, so the JVM is never killed mid-batch. read_stream.py
# installs its own handlers, so a signal that reaches it directly still stops
# the queries cleanly.
(
  trap '' INT TERM
  exec spark-submit \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_KAFKA_VERSION:-4.1.2}" \
    spark-streaming/read_stream.py
) &
spark_pid=$!

# A trapped signal interrupts wait (status > 128) while spark-submit is still
# running; keep waiting until it exits, then exit with its status.
set +e
wait "${spark_pid}"
status=$?
while [ "${status}" -gt 128 ] && kill -0 "${spark_pid}" 2>/dev/null; do
  wait "${spark_pid}"
  status=$?
done
set -e

exit "${status}"
