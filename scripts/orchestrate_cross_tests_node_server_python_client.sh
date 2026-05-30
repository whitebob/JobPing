#!/usr/bin/env bash
set -euo pipefail

# Cross-test: Node servers (control/experiment) x Python clients (control/experiment)
# Usage: COUNT=200 SLEEP=20 ./scripts/orchestrate_cross_tests_node_server_python_client.sh
#
# No standalone socket_broker.mjs needed — each experiment server embeds its own
# broker, and experiment clients connect directly to it via peer_brokers.

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [ -f ".venv/bin/activate" ]; then
  . .venv/bin/activate
fi

export PYTHONPATH="packages/python:sandbox/python:.:${PYTHONPATH:-}"

COUNT=${COUNT:-100}
SLEEP=${SLEEP:-1}
LOGDIR=${LOGDIR:-tmp/cross_test_logs_node_server_python_client}
BROKER_PORT=${BROKER_PORT:-8890}
BROKER_URL="http://127.0.0.1:${BROKER_PORT}"

mkdir -p "$LOGDIR"

PYTHON=${PYTHON:-.venv/bin/python3}
if [ ! -x "$PYTHON" ]; then
  PYTHON=python3
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

start_node_control_server() {
  echo "Starting node control server (port 8888)..."
  PORT=8888 node examples/control_group/server_node.mjs >"$LOGDIR/control_server_node.log" 2>&1 &
  NODE_SERVER_PID=$!
  echo "node control server pid=$NODE_SERVER_PID"
  if ! wait_for_http http://127.0.0.1:8888/metrics 10; then
    echo "Node control server failed to become ready"
    return 1
  fi
}

start_node_experiment_server() {
  echo "Starting node experiment server (port 8887)..."
  PORT=8887 BROKER_PORT="$BROKER_PORT" node examples/experiment_group/server_node.mjs >"$LOGDIR/experiment_server_node.log" 2>&1 &
  NODE_SERVER_PID=$!
  echo "node experiment server pid=$NODE_SERVER_PID"
  if ! wait_for_http http://127.0.0.1:8887/metrics 10; then
    echo "Node experiment server failed to become ready"
    return 1
  fi
}

run_pair() {
  local server_type=$1
  local client_type=$2
  local prefix="node_${server_type}_server__python_${client_type}_client"
  local out="$LOGDIR/${prefix}"
  echo ""
  echo "=== Running pair: server=node_${server_type} client=python_${client_type} ==="

  NODE_SERVER_PID=""

  if [ "$server_type" = "control" ]; then
    start_node_control_server || { return 1; }
    SERVER_URL="http://127.0.0.1:8888"
  else
    start_node_experiment_server || { return 1; }
    SERVER_URL="http://127.0.0.1:8887"
  fi

  sleep 0.5

  echo "Running python ${client_type} client against $SERVER_URL (COUNT=$COUNT SLEEP=$SLEEP)..."
  if [ "$client_type" = "control" ]; then
    SERVER_URL="$SERVER_URL" COUNT="$COUNT" SLEEP="$SLEEP" "$PYTHON" examples/control_group/client_py.py >"${out}.json" 2>"${out}.err" || true
  else
    SERVER_URL="$SERVER_URL" BROKER_URL="$BROKER_URL" COUNT="$COUNT" SLEEP="$SLEEP" "$PYTHON" examples/experiment_group/client_py.py >"${out}.json" 2>"${out}.err" || true
  fi

  sleep 2
  echo "Collecting metrics..."
  if curl -sS "$SERVER_URL/metrics" >"${out}_metrics.json" 2>/dev/null; then
    echo "Saved metrics to ${out}_metrics.json"
  else
    echo "Failed to fetch metrics from $SERVER_URL"
  fi

  kill_if_running "$NODE_SERVER_PID"

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
echo "All node/server + python/client pairs completed. Logs in $LOGDIR"
