#!/usr/bin/env bash
set -euo pipefail

# Orchestration script for cross-tests: runs server/client pairs and ensures
# proper startup/readiness and graceful shutdown.
# Usage: COUNT=100 SLEEP=1 ./scripts/orchestrate_cross_tests.sh

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

# Activate virtualenv if present
if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . .venv/bin/activate
fi

COUNT=${COUNT:-100}
SLEEP=${SLEEP:-1}
BROKER_PORT=${BROKER_PORT:-8890}
BROKER_URL="http://127.0.0.1:${BROKER_PORT}"

LOGDIR=./tmp/cross_test_logs
mkdir -p "$LOGDIR"

wait_for_http() {
  local url=$1
  local timeout=${2:-30}
  local start=$(date +%s)
  while true; do
    if curl --max-time 2 -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    if [ $(( $(date +%s) - start )) -ge $timeout ]; then
      return 1
    fi
    sleep 0.5
  done
}

wait_for_port() {
  local host=$1
  local port=$2
  local timeout=${3:-30}
  local start=$(date +%s)
  while true; do
    (echo > /dev/tcp/$host/$port) >/dev/null 2>&1 && return 0 || true
    if [ $(( $(date +%s) - start )) -ge $timeout ]; then
      return 1
    fi
    sleep 0.5
  done
}

start_broker() {
  echo "Starting broker..."
  node examples/experiment_group/socket_broker.mjs >"$LOGDIR/broker.log" 2>&1 &
  BROKER_PID=$!
  echo "broker pid=$BROKER_PID"
  if ! wait_for_port 127.0.0.1 "$BROKER_PORT" 10; then
    echo "Broker failed to start (port $BROKER_PORT not open)"
    return 1
  fi
  return 0
}

start_control_server() {
  echo "Starting control server (port 8888)..."
  WORKERS=${WORKERS:-1}
  PYTHONPATH=packages/python:sandbox/python:. .venv/bin/uvicorn --workers "$WORKERS" examples.control_group.server:app --host 127.0.0.1 --port 8888 >"$LOGDIR/control_server.log" 2>&1 &
  CONTROL_PID=$!
  echo "control pid=$CONTROL_PID (workers=$WORKERS)"
  if ! wait_for_http http://127.0.0.1:8888/metrics 10; then
    echo "Control server failed to become ready"
    return 1
  fi
  return 0
}

start_experiment_server() {
  echo "Starting experiment server (port 8887)..."
  WORKERS=${WORKERS:-1}
  PYTHONPATH=packages/python:sandbox/python:. .venv/bin/uvicorn --workers "$WORKERS" examples.experiment_group.server:app --host 127.0.0.1 --port 8887 >"$LOGDIR/experiment_server.log" 2>&1 &
  EXP_PID=$!
  echo "experiment pid=$EXP_PID (workers=$WORKERS)"
  if ! wait_for_http http://127.0.0.1:8887/metrics 10; then
    echo "Experiment server failed to become ready"
    return 1
  fi
  return 0
}

start_control_client() {
  local outfile=$1
  echo "Starting control client against $2 (count=$COUNT sleep=$SLEEP)..."
  node examples/control_group/client.mjs --serverUrl="$2" --count="$COUNT" --sleepSeconds="$SLEEP" >"$outfile" 2>&1 &
  CLIENT_PID=$!
  echo "control client pid=$CLIENT_PID"
}

start_experiment_client() {
  local outfile=$1
  echo "Starting experiment client against $2 with broker $BROKER_URL (count=$COUNT sleep=$SLEEP)..."
  node examples/experiment_group/client.mjs --serverUrl="$2" --brokerUrl="$BROKER_URL" --count="$COUNT" --sleepSeconds="$SLEEP" >"$outfile" 2>&1 &
  CLIENT_PID=$!
  echo "experiment client pid=$CLIENT_PID"
}

kill_if_running() {
  local pid=$1
  if [ -n "${pid:-}" ] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "Killing $pid"
    kill "$pid" || true
    wait "$pid" 2>/dev/null || true
  fi
}

run_pair() {
  local server_type=$1
  local client_type=$2
  local prefix="${server_type}_server__${client_type}_client"
  local out="$LOGDIR/${prefix}.out"
  echo "\n=== Running pair: server=$server_type client=$client_type ==="

  # Start broker if either side is experiment
  BROKER_PID=""
  CONTROL_PID=""
  EXP_PID=""
  CLIENT_PID=""

  if [ "$server_type" = "experiment" ] || [ "$client_type" = "experiment" ]; then
    start_broker || { echo "Failed to start broker"; return 1; }
  fi

  if [ "$server_type" = "control" ]; then
    start_control_server || { echo "Failed to start control server"; kill_if_running "$BROKER_PID"; return 1; }
    SERVER_URL="http://127.0.0.1:8888"
  else
    start_experiment_server || { echo "Failed to start experiment server"; kill_if_running "$BROKER_PID"; return 1; }
    SERVER_URL="http://127.0.0.1:8887"
  fi

  sleep 0.5

  if [ "$client_type" = "control" ]; then
    start_control_client "$out" "$SERVER_URL"
  else
    start_experiment_client "$out" "$SERVER_URL"
  fi

  echo "Waiting for client (pid=$CLIENT_PID) to finish (grace 30s)..."
  grace=30
  start=$(date +%s)
  while kill -0 "$CLIENT_PID" >/dev/null 2>&1; do
    if [ $(( $(date +%s) - start )) -ge $grace ]; then
      echo "Client did not exit after $grace s, killing..."
      kill_if_running "$CLIENT_PID"
      break
    fi
    sleep 0.5
  done

  echo "Client finished; collecting metrics..."
  if curl -sS "$SERVER_URL/metrics" >"$LOGDIR/${prefix}_metrics.json" 2>/dev/null; then
    echo "Saved metrics to $LOGDIR/${prefix}_metrics.json"
  else
    echo "Failed to fetch metrics from $SERVER_URL"
  fi

  # Stop server and broker
  kill_if_running "$CONTROL_PID"
  kill_if_running "$EXP_PID"
  kill_if_running "$BROKER_PID"

  echo "Pair ${prefix} finished; logs: $out"
}

# Pairs to run
pairs=(
  "control control"
  "control experiment"
  "experiment control"
  "experiment experiment"
)

for p in "${pairs[@]}"; do
  server_type=$(echo "$p" | awk '{print $1}')
  client_type=$(echo "$p" | awk '{print $2}')
  if ! run_pair "$server_type" "$client_type"; then
    echo "Run failed for pair: $p"
  fi
  # brief cooldown between runs
  sleep 0.5
done

echo "All pairs completed. Logs in $LOGDIR"
