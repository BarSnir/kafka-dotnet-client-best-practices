#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker compose up -d --wait prometheus grafana
printf '%s\n' 'Grafana: http://localhost:3000  Prometheus: http://localhost:9090'
