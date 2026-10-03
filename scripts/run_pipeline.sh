#!/bin/bash
# Batch-oriented end-to-end run (Docker already expected).
# For the interactive dashboard use ./start.sh instead.
set -e
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "[pipeline] starting Docker stack"
cd "$PROJECT_DIR"
docker compose up -d
bash "$PROJECT_DIR/scripts/download_clarknet.sh"

STREAM_DIR="/tmp/log_stream"
rm -rf "$STREAM_DIR"
mkdir -p "$STREAM_DIR"

echo "[pipeline] log replayer"
python3 "$PROJECT_DIR/ingestion/log_replayer.py" \
    --input "$PROJECT_DIR/data/raw/access_log" \
    --output-dir "$STREAM_DIR" \
    --speed 500 \
    --batch-size 100 \
    --inject-anomaly \
    --anomaly-type ddos \
    --anomaly-start 5000 \
    --anomaly-duration 2000 &
REPLAYER_PID=$!

sleep 3
echo "[pipeline] stream job"
python3 "$PROJECT_DIR/streaming/streaming_job.py" \
    --input-dir "$STREAM_DIR" \
    --mongo-uri "mongodb://localhost:27018/log_analytics" \
    --mode local &
STREAMING_PID=$!

wait $REPLAYER_PID 2>/dev/null || true
sleep 8
kill $STREAMING_PID 2>/dev/null || true

echo "[pipeline] MapReduce"
docker exec namenode bash /data/batch/run_mapreduce.sh || echo "[pipeline] MapReduce skipped"

echo "[pipeline] Hive"
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
    -f /data/batch/hive_tables.sql >/dev/null || echo "[pipeline] Hive skipped"

echo "[pipeline] R analytics"
if command -v Rscript >/dev/null 2>&1; then
    Rscript "$PROJECT_DIR/analytics/analysis.R" \
        --mongo-uri "mongodb://localhost:27018/log_analytics"
else
    echo "[pipeline] Rscript not installed — skip"
fi

echo "[pipeline] done. Dashboard: ./start.sh  ->  http://localhost:3000"
