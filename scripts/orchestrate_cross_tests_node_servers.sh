#!/usr/bin/env bash
set -euo pipefail

# Cross-test: Node servers (control/experiment) x Node clients (control/experiment)
# Usage: COUNT=200 SLEEP=20 ./scripts/orchestrate_cross_tests_node_servers.sh
#
# All components are Node.js. Each experiment server embeds its own broker;
# experiment clients connect directly to the server's broker.

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [ -f ".venv/bin/activate" ]; then
  . .venv/bin/activate
fi

COUNT=${COUNT:-100}
SLEEP=${SLEEP:-1}
BROKER_PORT=${BROKER_PORT:-8890}
BROKER_URL="http://127.0.0.1:${BROKER_PORT}"
LOGDIR=./tmp/cross_test_logs_node
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

start_node_control_server() {
  echo "Starting node control server (port 8888)..."
  PORT=8888 node examples/control_group/server_node.mjs >"$LOGDIR/control_server_node.log" 2>&1 &
  CONTROL_PID=$!
  echo "control node pid=$CONTROL_PID"
  if ! wait_for_http http://127.0.0.1:8888/metrics 10; then
    echo "Node control server failed to become ready"
    return 1
  fi
  return 0
}

start_node_experiment_server() {
  echo "Starting node experiment server (port 8887)..."
  PORT=8887 BROKER_PORT="$BROKER_PORT" node examples/experiment_group/server_node.mjs >"$LOGDIR/experiment_server_node.log" 2>&1 &
  EXP_PID=$!
  echo "experiment node pid=$EXP_PID"
  if ! wait_for_http http://127.0.0.1:8887/metrics 10; then
    echo "Node experiment server failed to become ready"
    return 1
  fi
  return 0
}

start_control_client() {
  local outfile=$1
  local serverUrl=$2
  echo "Starting control client against $serverUrl (count=$COUNT sleep=$SLEEP)..."
  node examples/control_group/client.mjs --serverUrl="$serverUrl" --count="$COUNT" --sleepSeconds="$SLEEP" >"$outfile" 2>&1 &
  CLIENT_PID=$!
  echo "control client pid=$CLIENT_PID"
}

start_experiment_client() {
  local outfile=$1
  local serverUrl=$2
  echo "Starting experiment client against $serverUrl with broker $BROKER_URL (count=$COUNT sleep=$SLEEP)..."
  node examples/experiment_group/client.mjs --serverUrl="$serverUrl" --brokerUrl="$BROKER_URL" --count="$COUNT" --sleepSeconds="$SLEEP" >"$outfile" 2>&1 &
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
  local prefix="node_${server_type}_server__${client_type}_client"
  local out="$LOGDIR/${prefix}.out"
  echo "\n=== Running pair: server=$server_type(client=node) client=$client_type ==="

  CONTROL_PID=""
  EXP_PID=""
  CLIENT_PID=""

  if [ "$server_type" = "control" ]; then
    start_node_control_server || { echo "Failed to start control node server"; return 1; }
    SERVER_URL="http://127.0.0.1:8888"
  else
    start_node_experiment_server || { echo "Failed to start experiment node server"; return 1; }
    SERVER_URL="http://127.0.0.1:8887"
  fi

  sleep 0.5

  if [ "$client_type" = "control" ]; then
    start_control_client "$out" "$SERVER_URL"
  else
    start_experiment_client "$out" "$SERVER_URL"
  fi

  echo "Waiting for client (pid=$CLIENT_PID) to finish (grace 30s)..."
  grace=60
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

  kill_if_running "$CONTROL_PID"
  kill_if_running "$EXP_PID"

  echo "Pair ${prefix} finished; logs: $out"
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

echo "All node/server pairs completed. Logs in $LOGDIR"
