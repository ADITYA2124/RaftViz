#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_PATH="$ROOT_DIR/target/RaftViz-0.0.1-SNAPSHOT.jar"
RUNTIME_DIR="$ROOT_DIR/demo-runtime"
DISCOVERY_GROUP="${RAFT_DISCOVERY_GROUP:-230.0.0.15}"
DISCOVERY_PORT="${RAFT_DISCOVERY_PORT:-4446}"
PORTS=(8080 8081 8082 8083)
NODE_IDS=(node-1 node-2 node-3 node-4)
PIDS=()

cleanup() {
  if [[ ${#PIDS[@]} -gt 0 ]]; then
    echo
    echo "Stopping demo nodes..."
    for pid in "${PIDS[@]}"; do
      kill "$pid" >/dev/null 2>&1 || true
    done
    wait >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local attempts="${2:-60}"
  local delay="${3:-1}"

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done

  echo "Timed out waiting for $url" >&2
  exit 1
}

start_node() {
  local index="$1"
  local node_id="${NODE_IDS[$index]}"
  local port="${PORTS[$index]}"
  local base_url="http://127.0.0.1:${port}"
  local log_file="$RUNTIME_DIR/${node_id}.log"

  mkdir -p "$RUNTIME_DIR"

  echo "Starting ${node_id} on ${base_url}"
  (
    cd "$ROOT_DIR"
    PORT="$port" \
    NODE_ID="$node_id" \
    ADVERTISE_ADDR="$base_url" \
    RAFT_DISCOVERY_GROUP="$DISCOVERY_GROUP" \
    RAFT_DISCOVERY_PORT="$DISCOVERY_PORT" \
    java -jar "$JAR_PATH" >"$log_file" 2>&1
  ) &

  PIDS+=("$!")
  wait_for_http "${base_url}/raft/state"
}

append_log() {
  local base_url="$1"
  local message="$2"

  curl -fsS -L \
    -H "Content-Type: application/json" \
    -d "{\"message\":\"${message}\"}" \
    "${base_url}/log" >/dev/null
}

print_banner() {
  cat <<EOF

RaftViz demo cluster is live.

Dashboard:
  http://127.0.0.1:8080/dashboard.html

Swagger UI:
  http://127.0.0.1:8080/swagger-ui/index.html

OpenAPI JSON:
  http://127.0.0.1:8080/v3/api-docs

What this demo does:
  1. Starts 3 Raft nodes.
  2. Waits for leader election.
  3. Appends sample log entries through the cluster.
  4. Starts node-4 later so the dashboard updates dynamically.
  5. Continues running until you press Ctrl+C.

EOF
}

main() {
  require_command java
  require_command curl

  cd "$ROOT_DIR"

  if [[ ! -f "$JAR_PATH" ]]; then
    echo "Building application jar..."
    chmod +x "$ROOT_DIR/mvnw" || true
    "$ROOT_DIR/mvnw" -q -DskipTests package
  fi

  print_banner

  start_node 0
  start_node 1
  start_node 2

  echo "Waiting for cluster discovery to settle..."
  sleep 6

  echo "Appending initial log entries..."
  append_log "http://127.0.0.1:8080" "create order ledger"
  append_log "http://127.0.0.1:8081" "replicate inventory snapshot"
  append_log "http://127.0.0.1:8082" "compact stale audit markers"

  echo "Starting late-joining node to demonstrate dynamic discovery..."
  start_node 3

  sleep 6

  echo "Appending more log entries after node-4 joins..."
  append_log "http://127.0.0.1:8083" "node 4 joined the cluster"
  append_log "http://127.0.0.1:8080" "commit payment batch"

  echo "Demo is running. Open the dashboard and watch node discovery and per-node logs update."
  while true; do
    sleep 5
  done
}

main "$@"
