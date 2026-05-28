#!/usr/bin/env bash
# Build all three JobPing distribution formats.
#
# Usage: bash scripts/build_dist.sh [--python-only|--js-only|--browser-only]
#
# Outputs:
#   packages/python/dist/jobping-*.whl            (pip)
#   packages/python/dist/jobping-*.tar.gz          (pip sdist)
#   packages/js/jobping-*.tgz                       (npm pack)
#   packages/js/dist/jobping_browser.mjs           (ESM)
#   packages/js/dist/jobping_browser.min.js        (IIFE, minified)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-all}"

build_browser() {
  echo "=== Building browser bundles ==="
  cd "$ROOT"
  node scripts/build_browser_bundle.mjs
  echo ""
}

build_npm() {
  echo "=== Building npm package ==="
  cd "$ROOT/packages/js"
  npm pack --pack-destination dist 2>&1 | tail -1
  echo ""
}

build_python() {
  echo "=== Building pip packages ==="
  cd "$ROOT/packages/python"
  "$ROOT/.venv/bin/python" -m build --outdir dist 2>&1 | tail -1
  echo ""
}

case "$MODE" in
  all)
    build_browser
    build_npm
    build_python
    ;;
  --browser-only)
    build_browser
    ;;
  --js-only)
    build_browser
    build_npm
    ;;
  --python-only)
    build_python
    ;;
  *)
    echo "Usage: bash scripts/build_dist.sh [--python-only|--js-only|--browser-only]"
    exit 1
    ;;
esac

echo "=== Done ==="
echo "pip:   packages/python/dist/"
echo "npm:   packages/js/dist/"
echo "min.js: packages/js/dist/jobping_browser.min.js"
