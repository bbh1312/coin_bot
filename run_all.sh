#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  if [[ -n "${WS_PID:-}" ]]; then
    kill "${WS_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

python "$ROOT_DIR/manage_ws.py" &
WS_PID=$!

ENGINE_ARGS=("$@")
NEED_NO_MANAGE_LOOP=1
for arg in "${ENGINE_ARGS[@]:-}"; do
  if [[ "$arg" == "--no-manage-loop" ]]; then
    NEED_NO_MANAGE_LOOP=0
    break
  fi
done
if [[ $NEED_NO_MANAGE_LOOP -eq 1 ]]; then
  ENGINE_ARGS+=("--no-manage-loop")
fi

python "$ROOT_DIR/engine_runner.py" "${ENGINE_ARGS[@]}"
