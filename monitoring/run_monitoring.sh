#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[monitoring] Project root: ${PROJECT_ROOT}"
echo "[monitoring] Using Docker host network for Linux reliability."

echo "[monitoring] Removing old monitoring containers..."
for name in kafka-exporter prometheus grafana feature-freshness-exporter; do
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
  -t project1-feature-freshness-exporter:latest \
  -f "${PROJECT_ROOT}/monitoring/exporters/Dockerfile.feature_exporter" \
  "${PROJECT_ROOT}"

echo "[monitoring] Starting Kafka exporter..."
docker run -d \
  --name kafka-exporter \
  --network host \
  danielqsj/kafka-exporter:latest \
  --kafka.server=localhost:9092 \
  --web.listen-address=:9308

echo "[monitoring] Starting feature freshness exporter..."
docker run -d \
  --name feature-freshness-exporter \
  --network host \
  -e EXPORTER_PORT=8001 \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_PORT=5433 \
  -e POSTGRES_DB="${POSTGRES_DB:-personalization_db}" \
  -e POSTGRES_USER="${POSTGRES_USER:-de_user}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-de_password}" \
  project1-feature-freshness-exporter:latest

echo "[monitoring] Starting Prometheus..."
docker run -d \
  --name prometheus \
  --network host \
  -v "${PROJECT_ROOT}/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:latest \
  --config.file=/etc/prometheus/prometheus.yml

echo "[monitoring] Starting Grafana..."
docker run -d \
  --name grafana \
  --network host \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -v "${PROJECT_ROOT}/monitoring/grafana/provisioning:/etc/grafana/provisioning:ro" \
  -v "${PROJECT_ROOT}/monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
  grafana/grafana-oss:latest


echo
echo "[monitoring] Waiting for Prometheus..."
for i in {1..30}; do
  if curl -fsS http://127.0.0.1:9090/-/ready >/dev/null 2>&1; then
    echo "[monitoring] Prometheus is ready."
    break
  fi
  echo "[monitoring] Prometheus not ready yet... attempt ${i}/30"
  sleep 2
done

echo
echo "[monitoring] Waiting for Grafana..."
for i in {1..45}; do
  if curl -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
    echo "[monitoring] Grafana is ready."
    break
  fi
  echo "[monitoring] Grafana not ready yet... attempt ${i}/45"
  sleep 2
done

echo
echo "[monitoring] Final health check:"
curl -fsS http://127.0.0.1:9090/-/ready || true
echo
curl -fsS http://127.0.0.1:3000/api/health || true
echo

echo
echo "[monitoring] Started."
echo "Prometheus: http://127.0.0.1:9090"
echo "Grafana:    http://127.0.0.1:3000"
echo "Grafana login: admin / admin"
echo
echo "Required:"
echo "- FastAPI running on 0.0.0.0:8000"
echo "- Kafka running on localhost:9092"
echo "- PostgreSQL running on localhost:5433"
