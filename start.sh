#!/usr/bin/env bash
# One-command launcher: dataset + stack + API + UI.
# Usage:  cd ~/projects/bdaproject && ./start.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.run"
mkdir -p "$LOG_DIR" "$PID_DIR" "$ROOT/data/raw"

UI_URL="http://localhost:3000"
API_URL="http://localhost:8000"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27018/log_analytics}"

log() { printf '%s %s\n' "[start]" "$*"; }
fail() { printf '%s %s\n' "[start] ERROR:" "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1"; }

need docker
need python3
need curl

if ! docker compose version >/dev/null 2>&1; then
  fail "docker compose is required"
fi

chmod +x "$ROOT/scripts/download_clarknet.sh" "$ROOT/scripts/run_pipeline.sh" "$ROOT/scripts/verify.sh" || true

log "ensuring ClarkNet dataset"
bash "$ROOT/scripts/download_clarknet.sh"

if [[ ! -d "$ROOT/.venv" ]]; then
  log "creating Python venv"
  python3 -m venv "$ROOT/.venv"
fi
# shellcheck disable=SC1091
source "$ROOT/.venv/bin/activate"
log "installing Python deps"
pip install -q -r "$ROOT/api/requirements.txt" -r "$ROOT/streaming/requirements.txt"

export PATH="$ROOT/node-v20.11.1-linux-x64/bin:$PATH"
if ! command -v node >/dev/null 2>&1; then
  fail "node/npm required for the dashboard (install Node 18+)"
fi
if [[ ! -d "$ROOT/frontend/node_modules" ]]; then
  log "installing frontend deps"
  (cd "$ROOT/frontend" && npm install --silent)
fi

log "starting Docker stack (HDFS, YARN, Hive, Spark, MongoDB)"
docker compose up -d >/dev/null
log "waiting for MongoDB on :27018"
for i in $(seq 1 60); do
  if python3 - <<'PY' >/dev/null 2>&1
from pymongo import MongoClient
MongoClient("mongodb://localhost:27018", serverSelectionTimeoutMS=1000).admin.command("ping")
PY
  then
    log "MongoDB is up"
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    fail "MongoDB did not become ready. Check: docker compose logs mongo"
  fi
  sleep 2
done

stop_pid() {
  local name="$1"
  local pidfile="$PID_DIR/${name}.pid"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile" || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 0.5
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
}

stop_pid api
stop_pid ui

log "starting API on :8000"
nohup python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --log-level error \
  >"$LOG_DIR/api.log" 2>&1 &
echo $! > "$PID_DIR/api.pid"

log "starting dashboard on :3000"
nohup npm --prefix "$ROOT/frontend" run dev -- --host 0.0.0.0 --port 3000 --clearScreen false \
  >"$LOG_DIR/frontend.log" 2>&1 &
echo $! > "$PID_DIR/ui.pid"

log "waiting for API and UI"
api_ok=0
ui_ok=0
for i in $(seq 1 40); do
  curl -sf "$API_URL/api/health" >/dev/null 2>&1 && api_ok=1 || true
  curl -sf "$UI_URL" >/dev/null 2>&1 && ui_ok=1 || true
  if [[ "$api_ok" -eq 1 && "$ui_ok" -eq 1 ]]; then
    break
  fi
  sleep 0.5
done
[[ "$api_ok" -eq 1 ]] || fail "API did not start. See logs/api.log"
[[ "$ui_ok" -eq 1 ]] || log "UI not answering yet — open $UI_URL anyway"

log "starting live ClarkNet replay"
curl -sf -X POST "$API_URL/api/demo/start" >/dev/null || log "demo start returned an error (see logs/api.log)"

open_browser() {
  if command -v wslview >/dev/null 2>&1; then
    wslview "$UI_URL" >/dev/null 2>&1 || true
  elif [[ -x /mnt/c/Windows/explorer.exe ]]; then
    /mnt/c/Windows/explorer.exe "$UI_URL" >/dev/null 2>&1 || true
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$UI_URL" >/dev/null 2>&1 || true
  fi
}
open_browser

cat <<EOF

  Log Analyzer is up.

  Dashboard  $UI_URL
  API        $API_URL/docs
  MongoDB    localhost:27018
  Hadoop UI  http://localhost:9870
  Spark UI   http://localhost:8080

  Stop:      ./stop.sh
  Logs:      logs/api.log  logs/frontend.log  logs/replayer.log  logs/stream.log

EOF
