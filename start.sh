#!/usr/bin/env bash
set -Eeuo pipefail

# ── Resolve ROOT as a true Linux path, never a UNC path ────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$ROOT"

# ── Ensure node/npm from the bundled installation ──────────────
if [[ -d "$ROOT/node-v20.11.1-linux-x64/bin" ]]; then
    export PATH="$ROOT/node-v20.11.1-linux-x64/bin:$PATH"
fi

LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.run"

mkdir -p "$LOG_DIR" "$PID_DIR"

DEFAULT_API_PORT="${API_PORT:-8000}"
DEFAULT_UI_PORT="${UI_PORT:-3000}"

find_free_port() {
    local port="$1"

    while ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":${port}$"; do
        port=$((port + 1))
    done

    echo "$port"
}

kill_pid_file() {
    local name="$1"
    local file="$PID_DIR/$name.pid"

    if [[ -f "$file" ]]; then
        local pid
        pid="$(cat "$file" 2>/dev/null || true)"

        if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $name (PID $pid)..."
            kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true

            for _ in {1..20}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.25
            done

            kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
        fi

        rm -f "$file"
    fi
}

cleanup() {
    trap - INT TERM EXIT

    echo
    echo "Stopping Log Analyzer..."

    # Stop demo workers through the API first.
    if [[ -n "${API_URL:-}" ]]; then
        curl -sf -X POST "$API_URL/api/demo/stop" >/dev/null 2>&1 || true
    fi

    kill_pid_file "ui"
    kill_pid_file "api"
    kill_pid_file "tunnel"
    rm -f "$PID_DIR/tunnel.url"

    echo "Local application stopped."

    # Stop Docker services belonging to this application.
    if command -v docker >/dev/null 2>&1; then
        docker compose down >/dev/null 2>&1 || true
        echo "Docker services stopped."
    fi

    echo "Everything stopped."
}

trap cleanup INT TERM EXIT

echo
echo "=========================================="
echo "       CLARKNET BIG DATA ANALYTICS"
echo "          LABORATORY"
echo "=========================================="
echo

# ------------------------------------------------------------
# Docker
# ------------------------------------------------------------

echo "[1/6] Starting infrastructure..."
docker compose up -d

# ------------------------------------------------------------
# HDFS Dataset – persist once, skip on subsequent starts
# ------------------------------------------------------------

echo "[2/6] Ensuring dataset is in HDFS..."

RAW_LOG="$ROOT/data/raw/access_log"
HDFS_PATH="/user/data/raw_logs/access_log"

# Wait for namenode to accept HDFS commands (up to 90s)
HDFS_READY=false
for i in $(seq 1 90); do
    if docker exec namenode hdfs dfs -ls / >/dev/null 2>&1; then
        HDFS_READY=true
        break
    fi
    sleep 1
done

if ! $HDFS_READY; then
    echo "WARNING: HDFS not ready after 90s – dataset upload skipped."
else
    # Check whether dataset already exists in HDFS (from a previous run)
    if docker exec namenode hdfs dfs -test -e "$HDFS_PATH" 2>/dev/null; then
        echo "  ✓ Dataset already present in HDFS (persisted from previous run)."
    elif [[ -f "$RAW_LOG" ]]; then
        echo "  Uploading dataset to HDFS (first-time setup)..."
        docker cp "$RAW_LOG" namenode:/tmp/access_log
        docker exec namenode bash -c "
            hdfs dfs -mkdir -p /user/data/raw_logs /user/data/results && \
            hdfs dfs -put -f /tmp/access_log $HDFS_PATH && \
            rm -f /tmp/access_log
        "
        echo "  ✓ Dataset uploaded to HDFS at $HDFS_PATH"
    else
        echo "  WARNING: Local dataset not found at $RAW_LOG – HDFS upload skipped."
    fi
fi

# ------------------------------------------------------------
# YARN ResourceManager — wait until it accepts HTTP connections
# Jobs submitted before YARN is ready fail with:
#   java.net.ConnectException: Connection refused (resourcemanager:8032)
#   java.net.UnknownHostException: Invalid host name: local host is: (unknown)
# ------------------------------------------------------------

echo "[2b/6] Waiting for YARN ResourceManager (up to 120s)..."
YARN_READY=false
for i in $(seq 1 120); do
    if curl -sf http://127.0.0.1:8088/ws/v1/cluster/info > /dev/null 2>&1; then
        YARN_READY=true
        echo "  ✓ YARN ResourceManager is ready (${i}s)."
        break
    fi
    sleep 1
done

if ! $YARN_READY; then
    echo "  WARNING: YARN ResourceManager not ready after 120s."
    echo "           MapReduce jobs will fail until it recovers."
    echo "           Check status with: docker compose ps"
fi

echo "[2c/6] Waiting for YARN NodeManager compute node (up to 60s)..."
NM_READY=false
for i in $(seq 1 60); do
    ACTIVE_NODES=$(curl -sf http://127.0.0.1:8088/ws/v1/cluster/metrics 2>/dev/null | grep -o '"activeNodes":[0-9]*' | cut -d: -f2 || echo 0)
    if [[ "${ACTIVE_NODES:-0}" -ge 1 ]]; then
        NM_READY=true
        echo "  ✓ YARN NodeManager registered ($ACTIVE_NODES compute node active)."
        break
    fi
    # If nodemanager container stopped, ensure it is brought up
    if ! docker ps --filter "name=nodemanager" --filter "status=running" -q 2>/dev/null | grep -q .; then
        docker compose up -d nodemanager >/dev/null 2>&1 || true
    fi
    sleep 2
done

if ! $NM_READY; then
    echo "  WARNING: No active YARN NodeManager detected after 60s."
    echo "           MapReduce jobs will remain queued until NodeManager registers."
fi

# ------------------------------------------------------------
# Ports
# ------------------------------------------------------------

API_PORT="$(find_free_port "$DEFAULT_API_PORT")"
UI_PORT="$(find_free_port "$DEFAULT_UI_PORT")"

API_URL="http://127.0.0.1:${API_PORT}"
UI_URL="http://127.0.0.1:${UI_PORT}"

echo
echo "API      : $API_URL"
echo "Frontend : $UI_URL"
echo

# ------------------------------------------------------------
# Clear old logs
# ------------------------------------------------------------

: > "$LOG_DIR/frontend.log"
: > "$LOG_DIR/replayer.log"
: > "$LOG_DIR/stream.log"
# api.log and MapReduce logs are append/rotated so a new start preserves the
# previous session's diagnostics.

# ------------------------------------------------------------
# Virtualenv
# ------------------------------------------------------------

VENV="$ROOT/.venv"
if [[ -d "$VENV" && -f "$VENV/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$VENV/bin/activate"
fi

# ------------------------------------------------------------
# API
# ------------------------------------------------------------

echo "[3/6] Starting API on port $API_PORT..."

setsid bash -c "
    cd '$ROOT'
    if [[ -f '$ROOT/.venv/bin/activate' ]]; then
        source '$ROOT/.venv/bin/activate'
    fi
    exec python3 -m uvicorn api.main:app \
        --host 0.0.0.0 \
        --port $API_PORT \
        --log-level info \
        --no-access-log
" >"$LOG_DIR/api.log" 2>&1 &

API_PID=$!
echo "$API_PID" > "$PID_DIR/api.pid"

# ------------------------------------------------------------
# Frontend  (stay in linux-native paths; never use UNC)
# ------------------------------------------------------------

echo "[4/6] Starting frontend on port $UI_PORT..."
echo "      API proxy → $API_URL"

FRONTEND_DIR="$ROOT/frontend"
VITE_BIN="$FRONTEND_DIR/node_modules/.bin/vite"

# Ensure node_modules exist
if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
    echo "      Installing frontend dependencies..."
    (cd "$FRONTEND_DIR" && npm install --prefer-offline --no-audit --no-fund 2>&1 | tail -5)
fi

# Use the vite binary directly (avoids npm wrapper which can re-invoke cmd.exe on WSL)
setsid bash -c "
    cd '$FRONTEND_DIR'
    export VITE_API_TARGET='$API_URL'
    export VITE_PORT='$UI_PORT'
    export PATH='$ROOT/node-v20.11.1-linux-x64/bin:\$PATH'
    exec '$VITE_BIN' \
        --host 0.0.0.0 \
        --port '$UI_PORT' \
        --strictPort \
        --clearScreen false
" >"$LOG_DIR/frontend.log" 2>&1 &

UI_PID=$!
echo "$UI_PID" > "$PID_DIR/ui.pid"

# ------------------------------------------------------------
# Wait for API
# ------------------------------------------------------------

echo "[5/6] Waiting for API..."

for _ in {1..60}; do
    if curl -sf "$API_URL/api/health" >/dev/null 2>&1; then
        break
    fi

    if ! kill -0 "$API_PID" 2>/dev/null; then
        echo
        echo "ERROR: API crashed during startup."
        cat "$LOG_DIR/api.log"
        exit 1
    fi

    sleep 0.5
done

if ! curl -sf "$API_URL/api/health" >/dev/null 2>&1; then
    echo "ERROR: API did not become ready."
    cat "$LOG_DIR/api.log"
    exit 1
fi

# ------------------------------------------------------------
# Wait for frontend
# ------------------------------------------------------------

echo "[6/6] Waiting for frontend..."

for _ in {1..60}; do
    if curl -sf "$UI_URL" >/dev/null 2>&1; then
        break
    fi

    if ! kill -0 "$UI_PID" 2>/dev/null; then
        echo
        echo "ERROR: Frontend crashed during startup."
        cat "$LOG_DIR/frontend.log"
        exit 1
    fi

    sleep 0.5
done

if ! curl -sf "$UI_URL" >/dev/null 2>&1; then
    echo "ERROR: Frontend did not become ready."
    cat "$LOG_DIR/frontend.log"
    exit 1
fi

echo
echo "=========================================="
echo "APPLICATION READY"
echo "=========================================="
echo
echo "Dashboard:"
echo "  $UI_URL"
echo
echo "API:"
echo "  $API_URL"
echo "  $API_URL/docs"
echo
echo "Logs:"
echo "  $LOG_DIR/api.log"
echo "  $LOG_DIR/frontend.log"
echo "  $LOG_DIR/replayer.log"
echo "  $LOG_DIR/stream.log"
echo "  $LOG_DIR/mapreduce.log (concise job lifecycle)"
echo "  $LOG_DIR/mapreduce-debug.log (driver and Hadoop diagnostics)"
echo
echo "Press Ctrl+C to stop EVERYTHING."
echo
echo "=========================================="
echo "LIVE LOGS"
echo "=========================================="
echo

# ------------------------------------------------------------
# Stay attached
# ------------------------------------------------------------

tail -F \
    "$LOG_DIR/api.log" \
    "$LOG_DIR/frontend.log" \
    "$LOG_DIR/replayer.log" \
    "$LOG_DIR/stream.log" \
    "$LOG_DIR/mapreduce.log"
