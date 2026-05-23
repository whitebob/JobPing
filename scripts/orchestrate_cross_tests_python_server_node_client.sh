#!/usr/bin/env bash
set -euo pipefail

# Cross-test: Python servers (control/experiment) x Node clients (control/experiment)
# Usage: COUNT=200 SLEEP=20 ./scripts/orchestrate_cross_tests_python_server_node_client.sh

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [ -f ".venv/bin/activate" ]; then
  . .venv/bin/activate
fi

export PYTHONPATH="packages/python:sandbox/python:.:${PYTHONPATH:-}"

COUNT=${COUNT:-100}
SLEEP=${SLEEP:-1}
LOGDIR=${LOGDIR:-tmp/cross_test_logs_python_server_node_client}
BROKER_PORT=${BROKER_PORT:-8890}
BROKER_URL="http://127.0.0.1:${BROKER_PORT}"

mkdir -p "$LOGDIR"

UVICORN=${UVICORN:-.venv/bin/uvicorn}
if [ ! -x "$UVICORN" ]; then
  UVICORN=uvicorn
fi

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

kill_if_running() {
  local pid=$1
  if [ -n "${pid:-}" ] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "Killing $pid"
    kill "$pid" || true
    wait "$pid" 2>/dev/null || true
  fi
}

start_broker() {
  echo "Starting broker..."
  node examples/experiment_group/socket_broker.mjs >"$LOGDIR/broker.log" 2>&1 &
  BROKER_PID=$!
  echo "broker pid=$BROKER_PID"
  local start=$(date +%s)
  while true; do
    (echo > /dev/tcp/127.0.0.1/${BROKER_PORT}) >/dev/null 2>&1 && return 0 || true
    if [ $(( $(date +%s) - start )) -ge 10 ]; then
      echo "Broker failed to start (port $BROKER_PORT not open)"
      return 1
    fi
    sleep 0.2
  done
}

start_python_control_server() {
  echo "Starting python control server (port 8888)..."
  $UVICORN examples.control_group.server:app --host 127.0.0.1 --port 8888 >"$LOGDIR/control_server_python.log" 2>&1 &
  PY_SERVER_PID=$!
  echo "python control server pid=$PY_SERVER_PID"
  if ! wait_for_http http://127.0.0.1:8888/metrics 15; then
    echo "Python control server failed to become ready"
    return 1
  fi
}

start_python_experiment_server() {
  echo "Starting python experiment server (port 8887)..."
  $UVICORN examples.experiment_group.server:app --host 127.0.0.1 --port 8887 >"$LOGDIR/experiment_server_python.log" 2>&1 &
  PY_SERVER_PID=$!
  echo "python experiment server pid=$PY_SERVER_PID"
  if ! wait_for_http http://127.0.0.1:8887/metrics 15; then
    echo "Python experiment server failed to become ready"
    return 1
  fi
}

run_pair() {
  local server_type=$1
  local client_type=$2
  local prefix="python_${server_type}_server__node_${client_type}_client"
  local out="$LOGDIR/${prefix}"
  echo ""
  echo "=== Running pair: server=python_${server_type} client=node_${client_type} ==="

  BROKER_PID=""
  PY_SERVER_PID=""

  if [ "$server_type" = "experiment" ] || [ "$client_type" = "experiment" ]; then
    start_broker || { echo "Failed to start broker"; return 1; }
  fi

  if [ "$server_type" = "control" ]; then
    start_python_control_server || { kill_if_running "$BROKER_PID"; return 1; }
    SERVER_URL="http://127.0.0.1:8888"
  else
    start_python_experiment_server || { kill_if_running "$BROKER_PID"; return 1; }
    SERVER_URL="http://127.0.0.1:8887"
  fi

  sleep 0.5

  echo "Running node ${client_type} client against $SERVER_URL (COUNT=$COUNT SLEEP=$SLEEP)..."
  if [ "$client_type" = "control" ]; then
    node examples/control_group/client.mjs --serverUrl="$SERVER_URL" --count="$COUNT" --sleepSeconds="$SLEEP" >"${out}.json" 2>"${out}.err" || true
  else
    node examples/experiment_group/client.mjs --serverUrl="$SERVER_URL" --brokerUrl="$BROKER_URL" --count="$COUNT" --sleepSeconds="$SLEEP" >"${out}.json" 2>"${out}.err" || true
  fi

  sleep 2
  echo "Collecting metrics..."
  if curl -sS "$SERVER_URL/metrics" >"${out}_metrics.json" 2>/dev/null; then
    echo "Saved metrics to ${out}_metrics.json"
  else
    echo "Failed to fetch metrics from $SERVER_URL"
  fi

  kill_if_running "$PY_SERVER_PID"
  kill_if_running "$BROKER_PID"

  echo "Pair ${prefix} finished"
}

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
  sleep 0.5
done

echo ""
echo "All python/server + node/client pairs completed. Logs in $LOGDIR"
