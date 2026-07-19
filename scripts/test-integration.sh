#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_dir/docker-compose.integration.yml"
project_name="ap-workflow-it-${PPID}-${RANDOM}"

cleanup() {
  docker compose -p "$project_name" -f "$compose_file" down --volumes --remove-orphans
}
trap cleanup EXIT INT TERM

docker compose -p "$project_name" -f "$compose_file" up -d --wait

postgres_address="$(docker compose -p "$project_name" -f "$compose_file" port postgres 5432)"
redis_address="$(docker compose -p "$project_name" -f "$compose_file" port redis 6379)"
postgres_port="${postgres_address##*:}"
redis_port="${redis_address##*:}"

export DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:${postgres_port}/freight_ap_integration"
export TEST_DATABASE_URL="$DATABASE_URL"
export REDIS_URL="redis://127.0.0.1:${redis_port}/0"
export AP_WORKFLOW_INTEGRATION_COMPOSE_FILE="$compose_file"
export AP_WORKFLOW_INTEGRATION_COMPOSE_PROJECT="$project_name"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/ap-workflow-integration-uv-cache}"

cd "$repo_dir"
if [[ "$#" -eq 0 ]]; then
  uv run --extra dev python -m pytest -m integration tests/integration
else
  uv run --extra dev python -m pytest -m integration "$@"
fi
