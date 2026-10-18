#!/usr/bin/env bash
# diagnose_mapreduce.sh
# Quick diagnostic + recovery script for the MapReduce pipeline.
# Run from WSL: bash scripts/diagnose_mapreduce.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
API_PORT="${API_PORT:-8000}"
API_URL="http://127.0.0.1:${API_PORT}"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}[OK]${NC}    $*"; }
fail() { echo -e "  ${RED}[FAIL]${NC}  $*"; }
warn() { echo -e "  ${YELLOW}[WARN]${NC}  $*"; }

echo ""
echo "======================================"
echo "  MapReduce Pipeline Diagnostics"
echo "======================================"
echo ""

# ── 1. Docker ──────────────────────────────────────────────────
echo "─── Docker Containers ───"
CONTAINERS=(namenode datanode datanode2 datanode3 resourcemanager nodemanager mongo)
ALL_OK=true
for c in "${CONTAINERS[@]}"; do
    STATUS=$(docker inspect --format '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
    if [ "$STATUS" = "running" ]; then
        ok "$c  →  running"
    else
        fail "$c  →  $STATUS  ← PROBLEM"
        ALL_OK=false
    fi
done

if ! $ALL_OK; then
    echo ""
    warn "Some containers are not running. Attempting restart…"
    cd "$ROOT"
    docker compose up -d
    echo ""
    echo "  Waiting 30s for services to start…"
    sleep 30
fi

echo ""
echo "─── HDFS ───"
if docker exec namenode hdfs dfs -test -e /user/data/raw_logs/access_log > /dev/null 2>&1; then
    ok "Dataset present at /user/data/raw_logs/access_log"
else
    fail "Dataset MISSING from HDFS"
    if [ -f "$ROOT/data/raw/access_log" ]; then
        warn "Local file exists — uploading…"
        docker cp "$ROOT/data/raw/access_log" namenode:/tmp/access_log
        docker exec namenode bash -c "
            hdfs dfs -mkdir -p /user/data/raw_logs /user/data/results &&
            hdfs dfs -put -f /tmp/access_log /user/data/raw_logs/access_log &&
            rm -f /tmp/access_log
        "
        ok "Upload complete"
    else
        fail "Local file also missing: $ROOT/data/raw/access_log"
    fi
fi

# Check for leftover stale staging / lock dirs that could block future runs
echo ""
echo "─── Stale HDFS Artifacts ───"
STALE=$(docker exec namenode hdfs dfs -ls /user/data/results/ 2>/dev/null | \
    grep -E '__staging_|__publish_lock|\.backup\.|\.failed\.' | awk '{print $NF}' || true)
if [ -z "$STALE" ]; then
    ok "No stale staging/lock/backup directories found"
else
    warn "Stale directories found (from crashed previous runs):"
    echo "$STALE"
    echo ""
    read -rp "  Delete these stale dirs? [y/N]: " CONFIRM
    if [[ "${CONFIRM,,}" == "y" ]]; then
        while IFS= read -r dir; do
            docker exec namenode hdfs dfs -rm -r "$dir" && ok "Removed $dir" || warn "Could not remove $dir"
        done <<< "$STALE"
    else
        warn "Skipped. These may block re-runs of the same task."
    fi
fi

echo ""
echo "─── YARN ResourceManager ───"
if curl -sf http://127.0.0.1:8088/ws/v1/cluster/info > /dev/null 2>&1; then
    ok "ResourceManager HTTP API reachable at :8088"
else
    fail "ResourceManager HTTP API NOT reachable at :8088"
fi

# Check YARN from inside namenode (the actual path jobs use)
YARN_INNER=$(docker exec namenode bash -c \
    "curl -sf --max-time 5 http://resourcemanager:8088/ws/v1/cluster/info > /dev/null 2>&1 && echo ok || echo fail" 2>/dev/null || echo "fail")
if [ "$YARN_INNER" = "ok" ]; then
    ok "ResourceManager reachable from namenode container (Docker DNS OK)"
else
    fail "ResourceManager NOT reachable from namenode container"
    echo "     This will cause: java.net.UnknownHostException / Connection refused"
    echo "     Fix: docker compose up -d   (restarts stopped containers)"
fi

echo ""
echo "─── Python in Namenode ───"
if docker exec namenode python3 --version > /dev/null 2>&1; then
    PY_VER=$(docker exec namenode python3 --version 2>&1)
    ok "python3 available: $PY_VER"
else
    fail "python3 NOT found in namenode container"
    echo "     Fix: bash scripts/install_python_in_nodemanager.sh"
fi

echo ""
echo "─── Python in NodeManager ───"
if docker exec nodemanager python3 --version > /dev/null 2>&1; then
    PY_VER=$(docker exec nodemanager python3 --version 2>&1)
    ok "python3 available: $PY_VER"
else
    fail "python3 NOT found in nodemanager container"
    echo "     Fix: bash scripts/install_python_in_nodemanager.sh"
fi

echo ""
echo "─── API Health ───"
if curl -sf "${API_URL}/api/health" > /dev/null 2>&1; then
    HEALTH=$(curl -sf "${API_URL}/api/health")
    ok "API responding: $HEALTH"
else
    warn "API not reachable at ${API_URL} (may not be started yet)"
fi

echo ""
echo "─── Batch Scripts in Container ───"
if docker exec namenode ls /tmp/batch/run_mapreduce.sh > /dev/null 2>&1; then
    ok "Batch scripts present in namenode:/tmp/batch/"
else
    warn "Batch scripts NOT in namenode — will be copied on next job run (normal if API hasn't run a job yet)"
fi

echo ""
echo "======================================"
echo "  Diagnostics Complete"
echo "======================================"
echo ""
echo "If ResourceManager was not reachable, run:"
echo "  cd $ROOT && docker compose up -d"
echo "Then wait 30-60s and re-run this script."
echo ""
