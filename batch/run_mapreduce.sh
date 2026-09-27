#!/bin/bash
# ============================================================
# Run Hadoop Streaming MapReduce job for status code counting
# ============================================================
# Execute from inside the namenode/resourcemanager container:
#   docker exec -it namenode bash /data/batch/run_mapreduce.sh
# ============================================================

set -e

echo "=== Hadoop Streaming MapReduce: Status Code Counter ==="

# Check if raw log data exists in HDFS
echo "[1/4] Checking for raw log data in HDFS..."
hdfs dfs -mkdir -p /user/data/raw_logs

# Upload raw log file if not already there
if ! hdfs dfs -test -e /user/data/raw_logs/access_log 2>/dev/null; then
    echo "[2/4] Uploading raw log file to HDFS..."
    if [ -f /data/data/raw/access_log ]; then
        hdfs dfs -put /data/data/raw/access_log /user/data/raw_logs/
    elif [ -f /data/data/raw/NASA_access_log_Jul95 ]; then
        hdfs dfs -put /data/data/raw/NASA_access_log_Jul95 /user/data/raw_logs/access_log
    else
        echo "ERROR: No raw log file found in /data/data/raw/"
        echo "Available files:"
        ls -la /data/data/raw/ 2>/dev/null || echo "  Directory not found"
        exit 1
    fi
else
    echo "[2/4] Raw log data already in HDFS, skipping upload."
fi

# Remove old output if exists
echo "[3/4] Cleaning old output..."
hdfs dfs -rm -r -f /user/data/status_counts

# Find the hadoop-streaming jar
STREAMING_JAR=$(find /opt/hadoop*/share/hadoop/tools/lib/ -name "hadoop-streaming-*.jar" 2>/dev/null | head -1)
if [ -z "$STREAMING_JAR" ]; then
    echo "ERROR: hadoop-streaming jar not found"
    exit 1
fi
echo "Using streaming jar: $STREAMING_JAR"

# Run the MapReduce job
echo "[4/4] Running MapReduce job..."
hadoop jar "$STREAMING_JAR" \
    -input /user/data/raw_logs \
    -output /user/data/status_counts \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file /data/batch/mapreduce_job/mapper.py \
    -file /data/batch/mapreduce_job/reducer.py

echo ""
echo "=== MapReduce Output ==="
hdfs dfs -cat /user/data/status_counts/part-*
echo ""
echo "=== Done ==="
