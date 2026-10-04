#!/bin/bash
# Run a Hadoop Streaming MapReduce job over the ClarkNet log stored in HDFS.
set -euo pipefail

TASK="${1:-${TASK:-status}}"
case "$TASK" in
  status)
    TITLE="Status Code Counter"
    MAPPER="status_mapper.py"
    OUTPUT="/user/data/results/status_counts"
    ;;
  hosts)
    TITLE="Top Host Counter"
    MAPPER="host_mapper.py"
    OUTPUT="/user/data/results/top_hosts"
    ;;
  endpoints)
    TITLE="Top Endpoint Counter"
    MAPPER="endpoint_mapper.py"
    OUTPUT="/user/data/results/top_endpoints"
    ;;
  *)
    echo "ERROR: unknown MapReduce task '$TASK' (expected: status, hosts, endpoints)"
    exit 1
    ;;
esac

echo "=== Hadoop Streaming MapReduce: $TITLE ==="
export HADOOP_CLASSPATH=/opt/hadoop-3.2.1/share/hadoop/mapreduce/*:/opt/hadoop-3.2.1/share/hadoop/mapreduce/lib/*

echo "[1/4] Ensuring raw ClarkNet log exists in HDFS..."
hdfs dfs -mkdir -p /user/data/raw_logs /user/data/results
if ! hdfs dfs -test -e /user/data/raw_logs/access_log 2>/dev/null; then
    if [ -f /tmp/access_log ]; then
        hdfs dfs -put /tmp/access_log /user/data/raw_logs/access_log
    else
        echo "ERROR: No raw log file found in HDFS or /tmp/access_log"
        exit 1
    fi
else
    echo "Raw log already present in HDFS."
fi

echo "[2/4] Cleaning old task output: $OUTPUT"
hdfs dfs -rm -r -f "$OUTPUT"

STREAMING_JAR=$(find /opt/hadoop*/share/hadoop/tools/lib/ -name "hadoop-streaming-*.jar" 2>/dev/null | head -1)
if [ -z "$STREAMING_JAR" ]; then
    echo "ERROR: hadoop-streaming jar not found"
    exit 1
fi

echo "[3/4] Running mapper=$MAPPER reducer=reducer.py"
hadoop jar "$STREAMING_JAR" \
    -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.map.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.reduce.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.application.classpath="/opt/hadoop-3.2.1/share/hadoop/mapreduce/*:/opt/hadoop-3.2.1/share/hadoop/mapreduce/lib/*" \
    -input /user/data/raw_logs/access_log \
    -output "$OUTPUT" \
    -mapper "python3 $MAPPER" \
    -reducer "python3 reducer.py" \
    -file "/tmp/batch/mapreduce_job/$MAPPER" \
    -file /tmp/batch/mapreduce_job/reducer.py

echo "[4/4] Output: $OUTPUT"
hdfs dfs -cat "$OUTPUT"/part-* | sort -k2,2nr | head -50

echo "=== Done ==="
