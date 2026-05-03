#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_PATH="$ROOT_DIR/target/RaftViz-0.0.1-SNAPSHOT.jar"
RUNTIME_DIR="$ROOT_DIR/demo-runtime"

NODE_ID="${NODE_ID:-${1:-node-1}}"
PORT="${PORT:-8080}"
DISCOVERY_PORT="${RAFT_DISCOVERY_PORT:-4446}"
LAN_SCAN_INTERVAL_MS="${RAFT_DISCOVERY_LAN_SCAN_INTERVAL_MS:-3000}"
STALE_AFTER_MS="${RAFT_DISCOVERY_STALE_AFTER_MS:-15000}"
ELECTION_TIMEOUT_MIN="${ELECTION_TIMEOUT_MIN:-5000}"
ELECTION_TIMEOUT_MAX="${ELECTION_TIMEOUT_MAX:-9000}"
HEARTBEAT_INTERVAL="${HEARTBEAT_INTERVAL:-1000}"
PIDS=()

cleanup() {
  if [[ ${#PIDS[@]} -gt 0 ]]; then
    echo
    echo "Stopping ${NODE_ID}..."
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

usage() {
  cat <<EOF
Usage:
  bash scripts/demo-raft.sh node-1
  bash scripts/demo-raft.sh node-2

Run this script on both laptops. Laptop A should use node-1, laptop B should use node-2.

Optional overrides:
  ADVERTISE_ADDR=http://192.168.29.186:8080 bash scripts/demo-raft.sh node-1
  PORT=8080 bash scripts/demo-raft.sh node-2
EOF
}

validate_node_id() {
  if [[ "$NODE_ID" != "node-1" && "$NODE_ID" != "node-2" ]]; then
    usage
    echo
    echo "NODE_ID must be node-1 or node-2 for this network demo." >&2
    exit 1
  fi
}

detect_lan_ip() {
  if [[ -n "${ADVERTISE_ADDR:-}" ]]; then
    echo "$ADVERTISE_ADDR" | sed -E 's#^https?://([^:/]+).*#\1#'
    return 0
  fi

  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command \
      "Get-NetIPAddress -AddressFamily IPv4 | Where-Object { \$_.IPAddress -notlike '127.*' -and \$_.IPAddress -notlike '169.254.*' -and \$_.IPAddress -notmatch '^172\.(1[6-9]|2[0-9]|3[0-1])\.' } | Sort-Object @{Expression={ if (\$_.IPAddress -like '192.168.*') { 0 } elseif (\$_.IPAddress -like '10.*') { 1 } else { 2 } }}, InterfaceMetric | Select-Object -First 1 -ExpandProperty IPAddress" \
      | tr -d '\r' \
      | head -n 1
    return 0
  fi

  if command -v hostname >/dev/null 2>&1; then
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | grep -v '^127\.' | head -n 1
    return 0
  fi

  return 1
}

wait_for_http() {
  local url="$1"
  local attempts="${2:-90}"
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

cluster_has_both_nodes() {
  local base_url="$1"
  local snapshot
  snapshot="$(curl -fsS "${base_url}/raft/cluster/state" 2>/dev/null || true)"
  [[ "$snapshot" == *'"nodeId":"node-1"' && "$snapshot" == *'"nodeId":"node-2"' ]]
}

wait_for_two_node_cluster() {
  local base_url="$1"
  local attempts="${2:-80}"

  echo "Waiting until node-1 and node-2 are visible on this dashboard..."
  for ((i = 1; i <= attempts; i++)); do
    if cluster_has_both_nodes "$base_url"; then
      echo "Both nodes are visible in the dashboard snapshot."
      return 0
    fi
    sleep 2
  done

  echo "Could not see both node-1 and node-2 yet." >&2
  echo "Open ${base_url}/raft/cluster/state to inspect the current snapshot." >&2
  exit 1
}

append_log() {
  local base_url="$1"
  local message="$2"

  curl -fsS -L \
    -H "Content-Type: application/json" \
    -d "{\"message\":\"${message}\"}" \
    "${base_url}/log" >/dev/null
}

trigger_election() {
  local base_url="$1"

  echo "Triggering election from ${NODE_ID}..."
  curl -fsS -X POST "${base_url}/raft/simulate/trigger-election" >/dev/null || true
  sleep 4
}

start_node() {
  local base_url="$1"
  local log_file="$RUNTIME_DIR/${NODE_ID}.log"

  mkdir -p "$RUNTIME_DIR"

  echo "Starting ${NODE_ID} on ${base_url}"
  (
    cd "$ROOT_DIR"
    PORT="$PORT" \
    NODE_ID="$NODE_ID" \
    ADVERTISE_ADDR="$base_url" \
    RAFT_DISCOVERY_PORT="$DISCOVERY_PORT" \
    RAFT_DISCOVERY_LAN_SCAN_ENABLED=true \
    RAFT_DISCOVERY_LAN_SCAN_INTERVAL_MS="$LAN_SCAN_INTERVAL_MS" \
    RAFT_DISCOVERY_STALE_AFTER_MS="$STALE_AFTER_MS" \
    ELECTION_TIMEOUT_MIN="$ELECTION_TIMEOUT_MIN" \
    ELECTION_TIMEOUT_MAX="$ELECTION_TIMEOUT_MAX" \
    HEARTBEAT_INTERVAL="$HEARTBEAT_INTERVAL" \
    java -jar "$JAR_PATH" >"$log_file" 2>&1
  ) &

  PIDS+=("$!")
  wait_for_http "${base_url}/raft/state"
}

print_banner() {
  local base_url="$1"

  cat <<EOF

RaftViz two-laptop network demo is live.

This laptop:
  ${NODE_ID} -> ${base_url}

Dashboard:
  ${base_url}/dashboard.html

What this script does:
  1. Starts this laptop as ${NODE_ID}.
  2. Uses LAN discovery to find the other laptop.
  3. Waits until node-1 and node-2 are visible on the dashboard.
  4. Triggers a leader election.
  5. Sends sample log entries through the cluster.
  6. Keeps running until you press Ctrl+C.

Run on the other laptop with the opposite node id:
  bash scripts/demo-raft.sh node-1
  bash scripts/demo-raft.sh node-2

EOF
}

main() {
  require_command java
  require_command curl
  validate_node_id

  cd "$ROOT_DIR"

  if [[ ! -f "$JAR_PATH" ]]; then
    echo "Building application jar..."
    chmod +x "$ROOT_DIR/mvnw" || true
    "$ROOT_DIR/mvnw" -q -DskipTests package
  fi

  local_ip="$(detect_lan_ip || true)"
  if [[ -z "$local_ip" ]]; then
    echo "Could not detect a LAN IP. Run with ADVERTISE_ADDR=http://YOUR_LAN_IP:${PORT}" >&2
    exit 1
  fi

  base_url="${ADVERTISE_ADDR:-http://${local_ip}:${PORT}}"

  print_banner "$base_url"
  start_node "$base_url"
  wait_for_two_node_cluster "$base_url"

  if [[ "$NODE_ID" == "node-1" ]]; then
    trigger_election "$base_url"

    echo "Appending demo log entries. They should appear in the dashboard log table."
    append_log "$base_url" "network demo started by node-1"
    append_log "$base_url" "node-1 and node-2 discovered over LAN"
    append_log "$base_url" "leader election simulated from node-1"
  else
    echo "node-2 is visible and waiting. node-1 will trigger election and append demo logs."
  fi

  echo "Demo is running. Keep ${base_url}/dashboard.html open to watch topology, roles, and logs."
  while true; do
    sleep 5
  done
}

main "$@"
