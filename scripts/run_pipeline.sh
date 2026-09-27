#!/bin/bash
# ============================================================
# Master Pipeline Script — Run Everything End-to-End
# ============================================================
# Usage: bash scripts/run_pipeline.sh
# ============================================================

set -e
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "╔══════════════════════════════════════════════════╗"
echo "║  BDA Pipeline — Real-Time Log Analytics          ║"
echo "╚══════════════════════════════════════════════════╝"

# ─── Step 1: Start Docker Stack ─────────────────────────────
echo ""
echo "=== Step 1: Starting Docker stack ==="
cd "$PROJECT_DIR"
docker compose up -d
echo "Waiting 30s for services to initialize..."
sleep 30

# Check container status
echo ""
echo "=== Container Status ==="
docker compose ps

# ─── Step 2: Create stream directory ────────────────────────
echo ""
echo "=== Step 2: Setting up directories ==="
STREAM_DIR="/tmp/log_stream"
rm -rf "$STREAM_DIR"
mkdir -p "$STREAM_DIR"
echo "Stream directory: $STREAM_DIR"

# ─── Step 3: Start Log Replayer (background) ────────────────
echo ""
echo "=== Step 3: Starting Log Replayer ==="
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
echo "Replayer started (PID: $REPLAYER_PID)"

# ─── Step 4: Start Streaming Job (background) ───────────────
echo ""
echo "=== Step 4: Starting Streaming Processor ==="
sleep 5  # Let some files accumulate
python3 "$PROJECT_DIR/streaming/streaming_job.py" \
    --input-dir "$STREAM_DIR" \
    --mongo-uri "mongodb://localhost:27018/log_analytics" \
    --mode local &
STREAMING_PID=$!
echo "Streaming processor started (PID: $STREAMING_PID)"

# ─── Step 5: Wait for processing ────────────────────────────
echo ""
echo "=== Step 5: Waiting for data processing... ==="
echo "Replayer PID: $REPLAYER_PID"
echo "Streaming PID: $STREAMING_PID"
echo "Press Ctrl+C to stop watching (processes continue in background)"
echo ""

# Wait for replayer to finish
wait $REPLAYER_PID 2>/dev/null || true
echo ""
echo "Replayer finished! Waiting 10s for streaming to catch up..."
sleep 10

# Stop streaming processor
kill $STREAMING_PID 2>/dev/null || true
echo "Streaming processor stopped."

# ─── Step 6: Run MapReduce Job ──────────────────────────────
echo ""
echo "=== Step 6: Running Hadoop MapReduce Job ==="
docker exec namenode bash /data/batch/run_mapreduce.sh || echo "MapReduce job skipped/failed"

# ─── Step 7: Run Hive Queries ───────────────────────────────
echo ""
echo "=== Step 7: Running Hive Table Setup ==="
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
    -f /data/batch/hive_tables.sql 2>/dev/null || echo "Hive setup skipped/failed"

# ─── Step 8: Run R Analytics ────────────────────────────────
echo ""
echo "=== Step 8: Running R Analytics ==="
if command -v Rscript &> /dev/null; then
    Rscript "$PROJECT_DIR/analytics/analysis.R" \
        --mongo-uri "mongodb://localhost:27018/log_analytics"
else
    echo "Rscript not found — install R or run manually later"
fi

# ─── Step 9: Start Dashboard ────────────────────────────────
echo ""
echo "=== Step 9: Starting Dashboard ==="
echo "Run: streamlit run $PROJECT_DIR/dashboard/app.py"
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Pipeline Complete! Open http://localhost:8501   ║"
echo "╚══════════════════════════════════════════════════╝"
