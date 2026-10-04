#!/bin/bash
# ============================================================
# Run Hadoop Streaming MapReduce job for status code counting
# ============================================================

set -e
echo "=== Hadoop Streaming MapReduce: Status Code Counter ==="

# ── Fix MRAppMaster classpath (bde2020 images miss this) ──
export HADOOP_CLASSPATH=/opt/hadoop-3.2.1/share/hadoop/mapreduce/*:/opt/hadoop-3.2.1/share/hadoop/mapreduce/lib/*

# Check if raw log data exists in HDFS
echo "[1/4] Checking for raw log data in HDFS..."
hdfs dfs -mkdir -p /user/data/raw_logs

if ! hdfs dfs -test -e /user/data/raw_logs/access_log 2>/dev/null; then
    echo "[2/4] Uploading raw log file to HDFS..."
    if [ -f /tmp/access_log ]; then
        hdfs dfs -put /tmp/access_log /user/data/raw_logs/access_log
    else
        echo "ERROR: No raw log file found in /tmp/access_log"
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
    -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.map.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.reduce.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.application.classpath="/opt/hadoop-3.2.1/share/hadoop/mapreduce/*:/opt/hadoop-3.2.1/share/hadoop/mapreduce/lib/*" \
    -input /user/data/raw_logs \
    -output /user/data/status_counts \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file /tmp/batch/mapreduce_job/mapper.py \
    -file /tmp/batch/mapreduce_job/reducer.py

echo ""
echo "=== MapReduce Output ==="
hdfs dfs -cat /user/data/status_counts/part-*
echo ""
echo "=== Done ==="
