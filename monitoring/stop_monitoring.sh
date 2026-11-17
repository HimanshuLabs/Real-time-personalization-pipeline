#!/usr/bin/env bash
set -euo pipefail

echo "[monitoring] Stopping monitoring containers..."

for name in project1_kafka_exporter project1_prometheus project1_grafana project1_feature_exporter; do
  id="$(docker ps -aq --filter "name=^/${name}$")"
  if [ -n "$id" ]; then
    echo "[monitoring] Removing ${name} -> ${id}"
    docker rm -f "$id"
  else
    echo "[monitoring] ${name} not found"
  fi
done

echo "[monitoring] Done."
