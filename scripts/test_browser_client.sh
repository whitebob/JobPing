#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

if [ -f ".venv/bin/activate" ]; then
  . .venv/bin/activate
fi

export PYTHONPATH="packages/python:sandbox/python:.:${PYTHONPATH:-}"

LOGDIR=./tmp/browser_test
mkdir -p "$LOGDIR"

# Start broker
echo "Starting broker..."
node examples/experiment_group/socket_broker.mjs >"$LOGDIR/broker.log" 2>&1 &
BROKER_PID=$!
sleep 1

if ! (echo > /dev/tcp/127.0.0.1/8890) 2>/dev/null; then
  echo "Broker failed to start"
  kill "$BROKER_PID" 2>/dev/null || true
  exit 1
fi
echo "Broker OK (pid=$BROKER_PID)"

# Start server
echo "Starting server..."
BROKER_URL=http://127.0.0.1:8890 uvicorn examples.experiment_group.server:app --host 127.0.0.1 --port 8887 >"$LOGDIR/server.log" 2>&1 &
SERVER_PID=$!
sleep 3

# Wait for server
for i in $(seq 1 15); do
  if curl -sS --max-time 1 http://127.0.0.1:8887/metrics >/dev/null 2>&1; then
    echo "Server OK (pid=$SERVER_PID)"
    break
  fi
  if [ "$i" -eq 15 ]; then
    echo "Server failed to start"
    cat "$LOGDIR/server.log"
    kill "$BROKER_PID" "$SERVER_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 0.5
done

echo ""
echo "=== Testing endpoints ==="

echo "--- GET / (HTML) ---"
curl -sS http://127.0.0.1:8887/ | head -3
echo ""

echo "--- GET /jobping_browser.mjs (JS bundle) ---"
curl -sS http://127.0.0.1:8887/jobping_browser.mjs | head -3
echo ""

echo "--- GET /work (with JP header) ---"
curl -sS -H "x-jobping-job-id: test-123" "http://127.0.0.1:8887/work?request_id=1&sleep_seconds=0.1"
echo ""

echo "--- GET /work (no JP header) ---"
curl -sS "http://127.0.0.1:8887/work?request_id=2&sleep_seconds=0.1"
echo ""

echo "--- GET /metrics ---"
curl -sS http://127.0.0.1:8887/metrics
echo ""

# Cleanup
kill "$SERVER_PID" "$BROKER_PID" 2>/dev/null || true
wait 2>/dev/null || true
echo ""
echo "All tests passed"
