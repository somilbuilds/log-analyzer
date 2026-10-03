#!/usr/bin/env bash
# Stop demo processes, API, and UI started by ./start.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
PID_DIR="$ROOT/.run"

stop_pid() {
  local name="$1"
  local pidfile="$PID_DIR/${name}.pid"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile" || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 0.3
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
    echo "[stop] $name"
  fi
}

if command -v curl >/dev/null 2>&1; then
  curl -sf -X POST http://localhost:8000/api/demo/stop >/dev/null 2>&1 || true
fi

stop_pid api
stop_pid ui

pkill -f "ingestion/log_replayer.py" 2>/dev/null || true
pkill -f "streaming/streaming_job.py" 2>/dev/null || true

echo "[stop] local processes stopped (Docker stack still running)"
echo "       docker compose down   # if you also want to stop Hadoop/Spark/Mongo"
