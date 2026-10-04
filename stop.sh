#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

PID_DIR="$ROOT/.run"

echo
echo "Stopping ClarkNet Big Data Analytics..."
echo

# Stop demo processes through API if possible.
API_PID_FILE="$PID_DIR/api.pid"

if [[ -f "$API_PID_FILE" ]]; then
    API_PID="$(cat "$API_PID_FILE" 2>/dev/null || true)"

    if [[ "$API_PID" =~ ^[0-9]+$ ]] && kill -0 "$API_PID" 2>/dev/null; then
        API_PORT="$(ss -ltnp 2>/dev/null |
            grep -oE "127\.0\.0\.1:[0-9]+" |
            grep -oE "[0-9]+$" |
            head -1 || true)"

        if [[ -n "$API_PORT" ]]; then
            curl -sf -X POST \
                "http://127.0.0.1:${API_PORT}/api/demo/stop" \
                >/dev/null 2>&1 || true
        fi
    fi
fi

stop_group() {
    local name="$1"
    local file="$PID_DIR/$name.pid"

    [[ -f "$file" ]] || return 0

    local pid
    pid="$(cat "$file" 2>/dev/null || true)"

    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        echo "Stopping $name..."

        # Kill the complete process group.
        kill -TERM -- "-$pid" 2>/dev/null || \
            kill -TERM "$pid" 2>/dev/null || true

        for _ in {1..20}; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.25
        done

        kill -KILL -- "-$pid" 2>/dev/null || \
            kill -KILL "$pid" 2>/dev/null || true
    fi

    rm -f "$file"
}

stop_group "ui"
stop_group "api"

echo "Stopping Docker infrastructure..."

docker compose down

echo
echo "Verifying..."

if pgrep -af "uvicorn.*api.main" >/dev/null 2>&1; then
    echo "WARNING: API process still exists."
else
    echo "API       ✓ stopped"
fi

if pgrep -af "vite" >/dev/null 2>&1; then
    echo "WARNING: Vite process still exists."
else
    echo "Frontend  ✓ stopped"
fi

if pgrep -af "log_replayer.py" >/dev/null 2>&1; then
    echo "WARNING: replayer still exists."
else
    echo "Replayer  ✓ stopped"
fi

if pgrep -af "streaming_job.py" >/dev/null 2>&1; then
    echo "WARNING: streaming worker still exists."
else
    echo "Streaming ✓ stopped"
fi

echo
echo "Application stopped."
echo
