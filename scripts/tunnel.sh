#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.run"

mkdir -p "$LOG_DIR" "$PID_DIR"

TUNNEL_LOG="$LOG_DIR/tunnel.log"
PID_FILE="$PID_DIR/tunnel.pid"
URL_FILE="$PID_DIR/tunnel.url"

copy_to_clipboard() {
    local text="$1"
    if [[ -x "/mnt/c/windows/system32/clip.exe" ]]; then
        printf "%s" "$text" | /mnt/c/windows/system32/clip.exe 2>/dev/null || true
    elif command -v clip.exe >/dev/null 2>&1; then
        printf "%s" "$text" | clip.exe 2>/dev/null || true
    elif command -v xclip >/dev/null 2>&1; then
        printf "%s" "$text" | xclip -selection clipboard 2>/dev/null || true
    fi
}

is_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid="$(cat "$PID_FILE" 2>/dev/null || true)"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

start_tunnel() {
    if is_running; then
        local existing_url=""
        if [[ -f "$URL_FILE" ]]; then
            existing_url="$(cat "$URL_FILE")"
        fi
        echo "Cloudflare Tunnel is already running (PID $(cat "$PID_FILE"))."
        if [[ -n "$existing_url" ]]; then
            echo "Public Tunnel URL: $existing_url"
            copy_to_clipboard "$existing_url"
            echo "(Copied to clipboard)"
        fi
        return 0
    fi

    echo "Checking local API health on port 8000..."
    if ! curl -sf http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
        echo "WARNING: Local API does not appear to be running on port 8000."
        echo "         Run ./start.sh in another terminal if you haven't already."
    else
        echo "✓ Local API is responsive."
    fi

    echo "Starting Cloudflare Quick Tunnel for http://localhost:8000..."
    : > "$TUNNEL_LOG"
    rm -f "$URL_FILE"

    # Start cloudflared in the background
    nohup /usr/local/bin/cloudflared tunnel --url http://localhost:8000 > "$TUNNEL_LOG" 2>&1 &
    local cf_pid=$!
    echo "$cf_pid" > "$PID_FILE"

    echo "Waiting for tunnel assignment..."
    local tunnel_url=""
    for i in $(seq 1 30); do
        if ! kill -0 "$cf_pid" 2>/dev/null; then
            echo "ERROR: cloudflared process exited unexpectedly. Log:"
            cat "$TUNNEL_LOG"
            rm -f "$PID_FILE"
            return 1
        fi

        tunnel_url="$(grep -o -E 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$TUNNEL_LOG" | head -n 1 || true)"
        if [[ -n "$tunnel_url" ]]; then
            echo "$tunnel_url" > "$URL_FILE"
            break
        fi
        sleep 0.5
    done

    if [[ -z "$tunnel_url" ]]; then
        echo "ERROR: Timed out waiting for Cloudflare Tunnel URL. Check $TUNNEL_LOG"
        return 1
    fi

    copy_to_clipboard "$tunnel_url"

    echo
    echo "============================================================"
    echo "  CLOUDFLARE TUNNEL ONLINE"
    echo "============================================================"
    echo "  Public URL : $tunnel_url"
    echo "  Forwarding : http://localhost:8000"
    echo "  Logs       : $TUNNEL_LOG"
    echo "  Status     : ✓ Copied to clipboard"
    echo "============================================================"
    echo
}

stop_tunnel() {
    if is_running; then
        local pid
        pid="$(cat "$PID_FILE")"
        echo "Stopping Cloudflare Tunnel (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        for _ in {1..10}; do
            if ! kill -0 "$pid" 2>/dev/null; then
                break
            fi
            sleep 0.2
        done
        kill -9 "$pid" 2>/dev/null || true
    else
        echo "Cloudflare Tunnel is not running."
    fi
    rm -f "$PID_FILE" "$URL_FILE"
    echo "Tunnel stopped."
}

status_tunnel() {
    if is_running; then
        local pid
        pid="$(cat "$PID_FILE")"
        local url=""
        if [[ -f "$URL_FILE" ]]; then
            url="$(cat "$URL_FILE")"
        fi
        echo "Tunnel: RUNNING (PID $pid)"
        echo "URL   : ${url:-unknown}"
        if [[ -n "$url" ]]; then
            echo "Health check through tunnel..."
            if curl -sf --max-time 6 "$url/api/health" >/dev/null 2>&1; then
                echo "Status: ✓ Tunnel is successfully proxying to local API!"
            else
                echo "Status: ⚠ Tunnel reachable, but API health check did not respond yet."
            fi
        fi
    else
        echo "Tunnel: STOPPED"
    fi
}

get_url() {
    if [[ -f "$URL_FILE" ]]; then
        cat "$URL_FILE"
    else
        echo ""
    fi
}

case "${1:-start}" in
    start)
        start_tunnel
        ;;
    stop)
        stop_tunnel
        ;;
    restart)
        stop_tunnel
        sleep 1
        start_tunnel
        ;;
    status)
        status_tunnel
        ;;
    url)
        get_url
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|url}"
        exit 1
        ;;
esac
