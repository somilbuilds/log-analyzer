#!/usr/bin/env bash
# One-command launcher: dataset + Docker stack + API + UI + live replay logs.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.run"
mkdir -p "$LOG_DIR" "$PID_DIR" "$ROOT/data/raw"

MONGO_URI="${MONGO_URI:-mongodb://localhost:27018/log_analytics}"
API_PORT="${API_PORT:-8000}"
UI_PORT="${UI_PORT:-3000}"

log() { printf '%s %s\n' "[start]" "$*"; }
fail() { printf '%s %s\n' "[start] ERROR:" "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1"; }

port_free() {
  local port="$1"
  python3 - "$port" <<'PY' >/dev/null 2>&1
import socket, sys
port = int(sys.argv[1])
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("127.0.0.1", port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

first_free_port() {
  local port="$1"
  while ! port_free "$port"; do
    log "port $port is occupied; trying $((port + 1))"
    port=$((port + 1))
  done
  printf '%s' "$port"
}

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

cleanup() {
  log "stopping local demo/API/UI processes"
  curl -sf -X POST "http://127.0.0.1:${API_PORT}/api/demo/stop" >/dev/null 2>&1 || true
  stop_pid api
  stop_pid ui
  [[ -n "${TAIL_PID:-}" ]] && kill "$TAIL_PID" 2>/dev/null || true
}
trap cleanup INT TERM

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
  [[ "$i" -eq 60 ]] && fail "MongoDB did not become ready. Check: docker compose logs mongo"
  sleep 2
done

API_PORT="$(first_free_port "$API_PORT")"
UI_PORT="$(first_free_port "$UI_PORT")"
API_URL="http://localhost:${API_PORT}"
UI_URL="http://localhost:${UI_PORT}"

stop_pid api
stop_pid ui
: > "$LOG_DIR/api.log"
: > "$LOG_DIR/frontend.log"
: > "$LOG_DIR/replayer.log"
: > "$LOG_DIR/stream.log"
: > "$LOG_DIR/mapreduce.log"

log "starting API on :${API_PORT}"
MONGO_URI="$MONGO_URI" nohup python3 -m uvicorn api.main:app --host 0.0.0.0 --port "$API_PORT" --log-level info \
  >"$LOG_DIR/api.log" 2>&1 &
echo $! > "$PID_DIR/api.pid"

log "starting dashboard on :${UI_PORT} with API proxy ${API_URL}"
VITE_PORT="$UI_PORT" VITE_API_TARGET="$API_URL" nohup npm --prefix "$ROOT/frontend" run dev -- --host 0.0.0.0 --port "$UI_PORT" --strictPort --clearScreen false \
  >"$LOG_DIR/frontend.log" 2>&1 &
echo $! > "$PID_DIR/ui.pid"

log "waiting for API and UI"
api_ok=0
ui_ok=0
for _ in $(seq 1 60); do
  curl -sf "$API_URL/api/health" >/dev/null 2>&1 && api_ok=1 || true
  curl -sf "$UI_URL" >/dev/null 2>&1 && ui_ok=1 || true
  [[ "$api_ok" -eq 1 && "$ui_ok" -eq 1 ]] && break
  sleep 0.5
done
[[ "$api_ok" -eq 1 ]] || fail "API did not start. See logs/api.log"
[[ "$ui_ok" -eq 1 ]] || fail "UI did not start. See logs/frontend.log"

log "starting live ClarkNet replay"
curl -sf -X POST "$API_URL/api/demo/start?resume=false" >/dev/null || log "demo start returned an error (see logs/api.log)"

cat <<EOF

  Log Analyzer is up.

  Dashboard  $UI_URL
  API docs   $API_URL/docs
  MongoDB    localhost:27018
  Hadoop UI  http://localhost:9870
  YARN UI    http://localhost:8088
  Spark UI   http://localhost:8080

  This terminal is now following live logs.
  Press Ctrl+C to stop the local API, UI, replayer, and stream worker.
  Docker services remain up; run: docker compose down

EOF

sleep 1
tail -n +1 -F "$LOG_DIR/api.log" "$LOG_DIR/frontend.log" "$LOG_DIR/replayer.log" "$LOG_DIR/stream.log" "$LOG_DIR/mapreduce.log" &
TAIL_PID=$!
wait "$TAIL_PID"
