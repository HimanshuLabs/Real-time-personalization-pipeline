#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Fixed tag, never :latest: bump it when the exporter or its requirements change,
# so the running container always names the exporter version it was built from.
EXPORTER_IMAGE="project1-feature-freshness-exporter:1.1"

echo "[monitoring] Project root: ${PROJECT_ROOT}"

GRAFANA_PASSWORD_GENERATED=false
if [ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]; then
  # head reads a fixed amount first, so no SIGPIPE under pipefail.
  GRAFANA_ADMIN_PASSWORD="$(head -c 256 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9' | cut -c1-24)"
  GRAFANA_PASSWORD_GENERATED=true
fi

echo "[monitoring] Using Docker host network for Linux reliability."
echo "[monitoring] Every listener binds to 127.0.0.1 only."

echo "[monitoring] Removing old monitoring containers..."
for name in project1_kafka_exporter project1_prometheus project1_grafana project1_feature_exporter; do
  id="$(docker ps -aq --filter "name=^/${name}$")"
  if [ -n "$id" ]; then
    echo "[monitoring] Removing ${name} -> ${id}"
    docker rm -f "$id"
  else
    echo "[monitoring] ${name} not found"
  fi
done

echo "[monitoring] Building feature freshness exporter..."
docker build \
  -t "${EXPORTER_IMAGE}" \
  -f "${PROJECT_ROOT}/monitoring/exporters/Dockerfile.feature_exporter" \
  "${PROJECT_ROOT}"

echo "[monitoring] Starting Kafka exporter..."
docker run -d \
  --name project1_kafka_exporter \
  --network host \
  danielqsj/kafka-exporter:v1.10.0 \
  --kafka.server=localhost:9092 \
  --web.listen-address=127.0.0.1:9308

echo "[monitoring] Starting feature freshness exporter..."
docker run -d \
  --name project1_feature_exporter \
  --network host \
  -e EXPORTER_PORT=8001 \
  -e EXPORTER_ADDR=127.0.0.1 \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_PORT=5433 \
  -e POSTGRES_DB="${POSTGRES_DB:-personalization_db}" \
  -e POSTGRES_USER="${POSTGRES_USER:-de_user}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-de_password}" \
  "${EXPORTER_IMAGE}"

echo "[monitoring] Starting Prometheus..."
docker run -d \
  --name project1_prometheus \
  --network host \
  -v "${PROJECT_ROOT}/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v3.5.4 \
  --config.file=/etc/prometheus/prometheus.yml \
  --web.listen-address=127.0.0.1:9090

echo "[monitoring] Starting Grafana..."
docker run -d \
  --name project1_grafana \
  --network host \
  -e GF_SERVER_HTTP_ADDR=127.0.0.1 \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD}" \
  -v "${PROJECT_ROOT}/monitoring/grafana/provisioning:/etc/grafana/provisioning:ro" \
  -v "${PROJECT_ROOT}/monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
  grafana/grafana-oss:13.0.2

# Polls a URL every 2 seconds. If it never answers, explains and exits 1, so
# "Started." is printed only when all four containers are serving.
wait_until_ready() {
  local label="$1" url="$2" attempts="$3" hint="$4"
  echo
  echo "[monitoring] Waiting for ${label}..."
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "[monitoring] ${label} is ready."
      return 0
    fi
    echo "[monitoring] ${label} not ready yet... attempt ${i}/${attempts}"
    sleep 2
  done
  echo "[monitoring] ${label} not ready after ${attempts} attempts (${url}).${hint:+ ${hint}}" >&2
  echo "[monitoring] Not started. Remove the containers with monitoring/stop_monitoring.sh." >&2
  exit 1
}

wait_until_ready "Prometheus" "http://127.0.0.1:9090/-/ready" 30 ""
wait_until_ready "Grafana" "http://127.0.0.1:3000/api/health" 45 ""
wait_until_ready "Kafka exporter" "http://127.0.0.1:9308/metrics" 15 \
  "Is Kafka running on localhost:9092? See: docker logs project1_kafka_exporter"
wait_until_ready "Feature freshness exporter" "http://127.0.0.1:8001/metrics" 15 \
  "See: docker logs project1_feature_exporter"

echo
echo "[monitoring] Final health check:"
curl -fsS http://127.0.0.1:9090/-/ready
echo
curl -fsS http://127.0.0.1:3000/api/health
echo

echo
echo "[monitoring] Started."
echo "Prometheus: http://127.0.0.1:9090"
echo "Grafana:    http://127.0.0.1:3000"
if [ "${GRAFANA_PASSWORD_GENERATED}" = true ]; then
  echo "Grafana login: admin / ${GRAFANA_ADMIN_PASSWORD}"
  echo "(generated; set GRAFANA_ADMIN_PASSWORD to choose your own)"
else
  echo "Grafana login: admin / (value of GRAFANA_ADMIN_PASSWORD)"
fi
echo
echo "Required:"
echo "- FastAPI running on 127.0.0.1:8000"
echo "- Kafka running on localhost:9092"
echo "- PostgreSQL running on localhost:5433"
