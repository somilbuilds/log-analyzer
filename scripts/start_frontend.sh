#!/usr/bin/env bash
set -e

ROOT="/home/dsomil/projects/bdaproject"
FRONTEND_DIR="$ROOT/frontend"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.run"

mkdir -p "$LOG_DIR" "$PID_DIR"

export PATH="$ROOT/node-v20.11.1-linux-x64/bin:/usr/bin:/bin:$PATH"
export VITE_API_TARGET="http://127.0.0.1:8000"
export VITE_PORT="3000"

cd "$FRONTEND_DIR"
nohup "$FRONTEND_DIR/node_modules/.bin/vite" \
    --host 0.0.0.0 \
    --port 3000 \
    --strictPort \
    --clearScreen false < /dev/null > "$LOG_DIR/frontend.log" 2>&1 &
UI_PID=$!
disown $UI_PID
echo "$UI_PID" > "$PID_DIR/ui.pid"
echo "Frontend started with PID $UI_PID on port 3000 pointing to http://127.0.0.1:8000"
