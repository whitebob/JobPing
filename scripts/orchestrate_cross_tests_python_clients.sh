#!/usr/bin/env bash
set -euo pipefail

# Cross-test orchestrator: Python servers (control/experiment) x Python clients (control/experiment)
# Usage: COUNT=100 SLEEP=1 WORKERS=1 ./scripts/orchestrate_cross_tests_python_clients.sh
#
# No standalone socket_broker.mjs needed — each experiment server embeds its own
# broker, and experiment clients connect directly to it via peer_brokers.

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  . .venv/bin/activate
fi

export PYTHONPATH="packages/python:sandbox/python:.:${PYTHONPATH:-}"

COUNT=${COUNT:-100}
SLEEP=${SLEEP:-1}
WORKERS=${WORKERS:-1}
GRACE=${GRACE:-30}
LOGDIR=${LOGDIR:-tmp/cross_test_logs_python}
BROKER_PORT=${BROKER_PORT:-8890}
BROKER_URL="http://127.0.0.1:${BROKER_PORT}"

mkdir -p "$LOGDIR"

CONTROL_PORT=${CONTROL_PORT:-8888}
EXPERIMENT_PORT=${EXPERIMENT_PORT:-8887}

UVICORN=${UVICORN:-.venv/bin/uvicorn}
if [ ! -x "$UVICORN" ]; then
  UVICORN=uvicorn
fi

PYTHON=${PYTHON:-.venv/bin/python3}
if [ ! -x "$PYTHON" ]; then
  PYTHON=python3
fi

wait_for_http() {
  local url=$1
  local timeout=${2:-30}
  local start
  start=$(date +%s)
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

start_server() {
  local kind=$1
  local port=$2
  local log=$3
  local pid

  if [ "$kind" = "control" ]; then
    $UVICORN examples.control_group.server:app --host 127.0.0.1 --port "$port" --workers "$WORKERS" >"$log" 2>&1 &
  else
    BROKER_PORT="$BROKER_PORT" $UVICORN examples.experiment_group.server:app --host 127.0.0.1 --port "$port" --workers "$WORKERS" >"$log" 2>&1 &
  fi
  pid=$!
  echo "$pid"
}

run_client() {
  local server_kind=$1
  local client_kind=$2
  local server_port=$3
  local server_url="http://127.0.0.1:${server_port}"
  local log_prefix="$LOGDIR/${server_kind}_server__${client_kind}_client"

  echo "Running ${server_kind} server + ${client_kind} client (COUNT=$COUNT SLEEP=$SLEEP)"

  if [ "$client_kind" = "control" ]; then
    SERVER_URL="$server_url" COUNT="$COUNT" SLEEP="$SLEEP" "$PYTHON" examples/control_group/client_py.py >"${log_prefix}.json" 2>"${log_prefix}.err" || true
  else
    SERVER_URL="$server_url" BROKER_URL="$BROKER_URL" COUNT="$COUNT" SLEEP="$SLEEP" "$PYTHON" examples/experiment_group/client_py.py >"${log_prefix}.json" 2>"${log_prefix}.err" || true
  fi
}

for server_kind in control experiment; do
  for client_kind in control experiment; do
    echo ""
    echo "=== Pair: server=$server_kind client=$client_kind ==="

    if [ "$server_kind" = "control" ]; then
      server_port="$CONTROL_PORT"
    else
      server_port="$EXPERIMENT_PORT"
    fi

    server_log="$LOGDIR/${server_kind}_server.log"
    SERVER_PID=$(start_server "$server_kind" "$server_port" "$server_log")
    echo "server pid=$SERVER_PID"

    echo "Waiting for server on http://127.0.0.1:${server_port}/metrics ..."
    if ! wait_for_http "http://127.0.0.1:${server_port}/metrics" 15; then
      echo "Server failed to become ready"
      kill_if_running "$SERVER_PID"
      continue
    fi
    sleep 0.5

    run_client "$server_kind" "$client_kind" "$server_port"

    # Collect metrics after client finishes
    sleep 2
    echo "Collecting metrics..."
    if curl -sS "http://127.0.0.1:${server_port}/metrics" >"$LOGDIR/${server_kind}_server__${client_kind}_client_metrics.json" 2>/dev/null; then
      echo "Saved metrics"
    else
      echo "Failed to fetch metrics"
    fi

    kill_if_running "$SERVER_PID"
    sleep 0.5
  done
done

echo ""
echo "All pairs completed. Logs in $LOGDIR"
