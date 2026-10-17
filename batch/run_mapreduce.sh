#!/bin/bash
# Run a Hadoop Streaming MapReduce job over the ClarkNet log stored in HDFS.
# Supports the full 25-task analytical catalog via dynamic mapper selection.
set -euo pipefail

TASK="${1:-${TASK:-status}}"

# ── Task → Mapper/Reducer/Output mapping ─────────────────────────
declare -A MAPPERS REDUCERS OUTPUTS TITLES

# Category 1 — Hosts & Volume
MAPPERS[hosts]=host_mapper.py;        REDUCERS[hosts]=reducer.py;           OUTPUTS[hosts]=/user/data/results/top_hosts;       TITLES[hosts]="Top Client IPs"
MAPPERS[endpoints]=endpoint_mapper.py; REDUCERS[endpoints]=reducer.py;      OUTPUTS[endpoints]=/user/data/results/top_endpoints; TITLES[endpoints]="Top Requested Paths"
MAPPERS[extensions]=extension_mapper.py; REDUCERS[extensions]=reducer.py;   OUTPUTS[extensions]=/user/data/results/extensions;   TITLES[extensions]="File Extension Distribution"
MAPPERS[methods]=method_mapper.py;     REDUCERS[methods]=reducer.py;        OUTPUTS[methods]=/user/data/results/methods;         TITLES[methods]="HTTP Method Distribution"
MAPPERS[hourly]=hour_mapper.py;        REDUCERS[hourly]=reducer.py;         OUTPUTS[hourly]=/user/data/results/hourly;           TITLES[hourly]="Requests by Hour"
MAPPERS[daily]=day_mapper.py;          REDUCERS[daily]=reducer.py;          OUTPUTS[daily]=/user/data/results/daily;             TITLES[daily]="Requests by Day"
MAPPERS[hour_day]=hour_day_mapper.py;  REDUCERS[hour_day]=reducer.py;       OUTPUTS[hour_day]=/user/data/results/hour_day;       TITLES[hour_day]="Day×Hour Heatmap"
MAPPERS[volume_time]=day_mapper.py;    REDUCERS[volume_time]=reducer.py;    OUTPUTS[volume_time]=/user/data/results/volume_time; TITLES[volume_time]="Request Volume Over Time"

# Category 2 — Responses & Errors
MAPPERS[status]=status_mapper.py;      REDUCERS[status]=reducer.py;         OUTPUTS[status]=/user/data/results/status_counts;   TITLES[status]="Status Code Distribution"
MAPPERS[status_4xx]=status4xx_mapper.py; REDUCERS[status_4xx]=reducer.py;   OUTPUTS[status_4xx]=/user/data/results/status_4xx;  TITLES[status_4xx]="4xx Client Errors"
MAPPERS[status_5xx]=status5xx_mapper.py; REDUCERS[status_5xx]=reducer.py;   OUTPUTS[status_5xx]=/user/data/results/status_5xx;  TITLES[status_5xx]="5xx Server Errors"
MAPPERS[error_paths]=error_path_mapper.py; REDUCERS[error_paths]=reducer.py; OUTPUTS[error_paths]=/user/data/results/error_paths; TITLES[error_paths]="Error-Producing Paths"
MAPPERS[error_hosts]=error_host_mapper.py; REDUCERS[error_hosts]=reducer.py; OUTPUTS[error_hosts]=/user/data/results/error_hosts; TITLES[error_hosts]="High-Error Clients"
MAPPERS[method_status]=method_status_mapper.py; REDUCERS[method_status]=method_status_reducer.py; OUTPUTS[method_status]=/user/data/results/method_status; TITLES[method_status]="Method × Status Cross-Tab"
MAPPERS[error_rate_day]=error_rate_day_mapper.py; REDUCERS[error_rate_day]=error_rate_day_reducer.py; OUTPUTS[error_rate_day]=/user/data/results/error_rate_day; TITLES[error_rate_day]="Error Rate Over Time"

# Category 3 — Traffic
MAPPERS[bytes_dist]=bytes_mapper.py;     REDUCERS[bytes_dist]=reducer.py;     OUTPUTS[bytes_dist]=/user/data/results/bytes_dist;   TITLES[bytes_dist]="Response Size Distribution"
MAPPERS[bytes_path]=bytes_total_mapper.py; REDUCERS[bytes_path]=sum_reducer.py; OUTPUTS[bytes_path]=/user/data/results/bytes_path; TITLES[bytes_path]="Bandwidth by Path"
MAPPERS[bytes_host]=host_bytes_mapper.py;  REDUCERS[bytes_host]=sum_reducer.py; OUTPUTS[bytes_host]=/user/data/results/bytes_host; TITLES[bytes_host]="Traffic Volume by Client"

# Category 4 — Security / Behavioral (each has its OWN unique output)
MAPPERS[high_rate_hosts]=host_mapper.py;     REDUCERS[high_rate_hosts]=reducer.py;   OUTPUTS[high_rate_hosts]=/user/data/results/high_rate_hosts;    TITLES[high_rate_hosts]="High-Rate Clients"
MAPPERS[failed_requests]=error_host_mapper.py; REDUCERS[failed_requests]=reducer.py; OUTPUTS[failed_requests]=/user/data/results/failed_requests;  TITLES[failed_requests]="Repeated Failed Requests"
MAPPERS[unusual_paths]=endpoint_mapper.py;    REDUCERS[unusual_paths]=reducer.py;    OUTPUTS[unusual_paths]=/user/data/results/unusual_paths;  TITLES[unusual_paths]="Rare Paths"
MAPPERS[suspicious_combos]=method_status_mapper.py; REDUCERS[suspicious_combos]=method_status_reducer.py; OUTPUTS[suspicious_combos]=/user/data/results/suspicious_combos; TITLES[suspicious_combos]="Suspicious Method+Status"
MAPPERS[rare_status]=status_mapper.py;        REDUCERS[rare_status]=reducer.py;      OUTPUTS[rare_status]=/user/data/results/rare_status;    TITLES[rare_status]="Rare Status Codes"
MAPPERS[host_concentration]=host_mapper.py;   REDUCERS[host_concentration]=reducer.py; OUTPUTS[host_concentration]=/user/data/results/host_concentration; TITLES[host_concentration]="Client Concentration"
MAPPERS[path_popularity]=endpoint_mapper.py;  REDUCERS[path_popularity]=reducer.py;   OUTPUTS[path_popularity]=/user/data/results/path_popularity; TITLES[path_popularity]="Path Popularity"
MAPPERS[malformed]=malformed_mapper.py;       REDUCERS[malformed]=reducer.py;         OUTPUTS[malformed]=/user/data/results/malformed;       TITLES[malformed]="Malformed Records"

# ── Validate task ─────────────────────────────────────────────────
MAPPER="${MAPPERS[$TASK]:-}"
REDUCER="${REDUCERS[$TASK]:-}"
OUTPUT="${OUTPUTS[$TASK]:-}"
TITLE="${TITLES[$TASK]:-}"

if [ -z "$MAPPER" ]; then
    echo "ERROR: unknown MapReduce task '$TASK'"
    echo "Available tasks: ${!MAPPERS[*]}"
    exit 1
fi

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

# Build file list — include all referenced scripts
FILE_ARGS="-file /tmp/batch/mapreduce_job/$MAPPER -file /tmp/batch/mapreduce_job/$REDUCER"

echo "[3/4] Running mapper=$MAPPER reducer=$REDUCER → output=$OUTPUT"
hadoop jar "$STREAMING_JAR" \
    -D dfs.client.use.datanode.hostname=false \
    -D yarn.app.mapreduce.am.resource.mb=512 \
    -D yarn.app.mapreduce.am.command-opts="-Xmx384m" \
    -D mapreduce.map.memory.mb=512 \
    -D mapreduce.map.java.opts="-Xmx384m" \
    -D mapreduce.reduce.memory.mb=512 \
    -D mapreduce.reduce.java.opts="-Xmx384m" \
    -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.map.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.reduce.env="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1" \
    -D mapreduce.application.classpath="/opt/hadoop-3.2.1/share/hadoop/mapreduce/*:/opt/hadoop-3.2.1/share/hadoop/mapreduce/lib/*" \
    -input /user/data/raw_logs/access_log \
    -output "$OUTPUT" \
    -mapper "python3 $MAPPER" \
    -reducer "python3 $REDUCER" \
    $FILE_ARGS

RC=$?
if [ $RC -ne 0 ]; then
    echo "ERROR: Hadoop Streaming job failed with exit code $RC"
    exit $RC
fi

echo "[4/4] Output: $OUTPUT"
hdfs dfs -cat "$OUTPUT"/part-* | head -50

echo "=== Done ==="
