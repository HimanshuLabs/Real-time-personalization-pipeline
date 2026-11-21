#!/usr/bin/env bash
set -euo pipefail

# Runs the Airflow CLI for this project, for example:
#   scripts/run_airflow.sh standalone
#   scripts/run_airflow.sh tasks test project1_personalization_batch_pipeline <task_id> <date>
#
# Airflow's own venv (venv-airflow) only schedules; every task runs on the
# project's venv (venv), which has PySpark, confluent-kafka and psycopg at the
# versions CI tests. The two stay separate because Airflow's official
# constraints pin other versions of packages this project pins.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$(pwd)"

if [ ! -x "${ROOT}/venv/bin/python" ]; then
  echo "[airflow] error: ${ROOT}/venv/bin/python not found; create the project venv first (README, Quick start)" >&2
  exit 1
fi
if [ ! -x "${ROOT}/venv-airflow/bin/airflow" ]; then
  echo "[airflow] error: ${ROOT}/venv-airflow/bin/airflow not found; install Airflow into venv-airflow (README, Operations, Airflow)" >&2
  exit 1
fi

# Tasks inherit this environment: spark-submit, python and python3 resolve to
# the project's venv, and the Spark driver uses the plain name "python", never
# a path, which may contain spaces.
export PATH="${ROOT}/venv/bin:${PATH}"
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
export AIRFLOW_HOME="${AIRFLOW_HOME:-${ROOT}/airflow_home}"

# A setting exported for another project's Airflow (its metadata database, its
# DAG folder) must not reach this one, so every inherited AIRFLOW__* variable
# is dropped and this Airflow is configured by its own airflow.cfg. AIRFLOW_HOME
# (one underscore) is kept. Names are printed, never values.
ignored=()
while IFS= read -r name; do
  ignored+=("${name}")
  unset "${name}"
done < <(compgen -e | grep '^AIRFLOW__' | LC_ALL=C sort || true)
if [ "${#ignored[@]}" -gt 0 ]; then
  echo "[airflow] ignoring inherited settings: ${ignored[*]}; this Airflow is configured by ${AIRFLOW_HOME}/airflow.cfg" >&2
fi

exec "${ROOT}/venv-airflow/bin/airflow" "$@"
