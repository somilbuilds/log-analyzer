#!/bin/bash
# Run a Hadoop Streaming MapReduce job over the ClarkNet log stored in HDFS.
# Supports the full 25-task analytical catalog via dynamic mapper selection.
set -euo pipefail

TASK="${1:-${TASK:-status}}"
BDA_JOB_ID="${BDA_JOB_ID:-local-$$}"
BDA_TASK_ID="$TASK"

log() {
    local level="$1"
    local stage="$2"
    shift 2
    printf '%s %s component=mapreduce-shell job_id=%s task_id=%s stage=%s %s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$level" "$BDA_JOB_ID" "$BDA_TASK_ID" "$stage" "$*"
}

run_hdfs() {
    local stage="$1"
    shift
    local output rc
    if output="$("$@" 2>&1)"; then
        rc=0
        log INFO "$stage" "operation=$* result=ok detail=${output:-none}"
    else
        rc=$?
        log ERROR "$stage" "operation=$* exit_code=$rc detail=${output:-no_error_output}"
    fi
    return "$rc"
}

# Return 0 for an existing path, 1 only for a confirmed missing path, and any
# other HDFS exit code for an indeterminate/failed check.
check_hdfs_path() {
    local path="$1" errfile rc detail
    errfile=$(mktemp)
    if hdfs dfs -test -e "$path" >/dev/null 2>"$errfile"; then
        rm -f "$errfile"
        return 0
    else
        rc=$?
    fi
    detail=$(cat "$errfile")
    rm -f "$errfile"
    if [ "$rc" -eq 1 ] && [ -z "$detail" ]; then
        return 1
    fi
    log ERROR hdfs_validation "operation=hdfs_dfs_test path=$path exit_code=$rc detail=${detail:-no_error_output}"
    return "$rc"
}

# ── Task → Mapper/Reducer/Output mapping ─────────────────────────
# Output paths are always /user/data/results/<task_id> so they match the
# Python catalog and every task has a guaranteed unique directory.
declare -A MAPPERS REDUCERS OUTPUTS TITLES

# Category 1 — Hosts & Volume
MAPPERS[hosts]=host_mapper.py;        REDUCERS[hosts]=reducer.py;           OUTPUTS[hosts]=/user/data/results/hosts;              TITLES[hosts]="Top Client IPs"
MAPPERS[endpoints]=endpoint_mapper.py; REDUCERS[endpoints]=reducer.py;      OUTPUTS[endpoints]=/user/data/results/endpoints;      TITLES[endpoints]="Top Requested Paths"
MAPPERS[extensions]=extension_mapper.py; REDUCERS[extensions]=reducer.py;   OUTPUTS[extensions]=/user/data/results/extensions;    TITLES[extensions]="File Extension Distribution"
MAPPERS[methods]=method_mapper.py;     REDUCERS[methods]=reducer.py;        OUTPUTS[methods]=/user/data/results/methods;           TITLES[methods]="HTTP Method Distribution"
MAPPERS[hourly]=hour_mapper.py;        REDUCERS[hourly]=reducer.py;         OUTPUTS[hourly]=/user/data/results/hourly;             TITLES[hourly]="Requests by Hour"
MAPPERS[daily]=day_mapper.py;          REDUCERS[daily]=reducer.py;          OUTPUTS[daily]=/user/data/results/daily;               TITLES[daily]="Requests by Day"
MAPPERS[hour_day]=hour_day_mapper.py;  REDUCERS[hour_day]=reducer.py;       OUTPUTS[hour_day]=/user/data/results/hour_day;         TITLES[hour_day]="Day×Hour Heatmap"
MAPPERS[volume_time]=day_mapper.py;    REDUCERS[volume_time]=reducer.py;    OUTPUTS[volume_time]=/user/data/results/volume_time;   TITLES[volume_time]="Request Volume Over Time"

# Category 2 — Responses & Errors
MAPPERS[status]=status_mapper.py;      REDUCERS[status]=reducer.py;         OUTPUTS[status]=/user/data/results/status;             TITLES[status]="Status Code Distribution"
MAPPERS[status_4xx]=status4xx_mapper.py; REDUCERS[status_4xx]=reducer.py;   OUTPUTS[status_4xx]=/user/data/results/status_4xx;    TITLES[status_4xx]="4xx Client Errors"
MAPPERS[status_5xx]=status5xx_mapper.py; REDUCERS[status_5xx]=reducer.py;   OUTPUTS[status_5xx]=/user/data/results/status_5xx;    TITLES[status_5xx]="5xx Server Errors"
MAPPERS[error_paths]=error_path_mapper.py; REDUCERS[error_paths]=reducer.py; OUTPUTS[error_paths]=/user/data/results/error_paths;  TITLES[error_paths]="Error-Producing Paths"
MAPPERS[error_hosts]=error_host_mapper.py; REDUCERS[error_hosts]=reducer.py; OUTPUTS[error_hosts]=/user/data/results/error_hosts;  TITLES[error_hosts]="High-Error Clients"
MAPPERS[method_status]=method_status_mapper.py; REDUCERS[method_status]=method_status_reducer.py; OUTPUTS[method_status]=/user/data/results/method_status; TITLES[method_status]="Method × Status Cross-Tab"
MAPPERS[error_rate_day]=error_rate_day_mapper.py; REDUCERS[error_rate_day]=error_rate_day_reducer.py; OUTPUTS[error_rate_day]=/user/data/results/error_rate_day; TITLES[error_rate_day]="Error Rate Over Time"

# Category 3 — Traffic
MAPPERS[bytes_dist]=bytes_mapper.py;     REDUCERS[bytes_dist]=reducer.py;     OUTPUTS[bytes_dist]=/user/data/results/bytes_dist;     TITLES[bytes_dist]="Response Size Distribution"
MAPPERS[bytes_path]=bytes_total_mapper.py; REDUCERS[bytes_path]=sum_reducer.py; OUTPUTS[bytes_path]=/user/data/results/bytes_path;   TITLES[bytes_path]="Bandwidth by Path"
MAPPERS[bytes_host]=host_bytes_mapper.py;  REDUCERS[bytes_host]=sum_reducer.py; OUTPUTS[bytes_host]=/user/data/results/bytes_host;   TITLES[bytes_host]="Traffic Volume by Client"

# Category 4 — Security / Behavioral
MAPPERS[high_rate_hosts]=host_mapper.py;     REDUCERS[high_rate_hosts]=reducer.py;   OUTPUTS[high_rate_hosts]=/user/data/results/high_rate_hosts;       TITLES[high_rate_hosts]="High-Rate Clients"
MAPPERS[failed_requests]=error_host_mapper.py; REDUCERS[failed_requests]=reducer.py; OUTPUTS[failed_requests]=/user/data/results/failed_requests;     TITLES[failed_requests]="Repeated Failed Requests"
MAPPERS[unusual_paths]=endpoint_mapper.py;    REDUCERS[unusual_paths]=reducer.py;    OUTPUTS[unusual_paths]=/user/data/results/unusual_paths;         TITLES[unusual_paths]="Rare Paths"
MAPPERS[suspicious_combos]=method_status_mapper.py; REDUCERS[suspicious_combos]=method_status_reducer.py; OUTPUTS[suspicious_combos]=/user/data/results/suspicious_combos; TITLES[suspicious_combos]="Suspicious Method+Status"
MAPPERS[rare_status]=status_mapper.py;        REDUCERS[rare_status]=reducer.py;      OUTPUTS[rare_status]=/user/data/results/rare_status;             TITLES[rare_status]="Rare Status Codes"
MAPPERS[host_concentration]=host_mapper.py;   REDUCERS[host_concentration]=reducer.py; OUTPUTS[host_concentration]=/user/data/results/host_concentration; TITLES[host_concentration]="Client Concentration"
MAPPERS[path_popularity]=endpoint_mapper.py;  REDUCERS[path_popularity]=reducer.py;   OUTPUTS[path_popularity]=/user/data/results/path_popularity;     TITLES[path_popularity]="Path Popularity"
MAPPERS[malformed]=malformed_mapper.py;       REDUCERS[malformed]=reducer.py;         OUTPUTS[malformed]=/user/data/results/malformed;                 TITLES[malformed]="Malformed Records"

# ── Validate task ─────────────────────────────────────────────────
MAPPER="${MAPPERS[$TASK]:-}"
REDUCER="${REDUCERS[$TASK]:-}"
OUTPUT="${OUTPUTS[$TASK]:-}"
TITLE="${TITLES[$TASK]:-}"

if [ -z "$MAPPER" ]; then
    log ERROR validation "reason=unknown_task available_tasks=${!MAPPERS[*]}"
    exit 1
fi

SAFE_JOB_ID=$(printf '%s' "$BDA_JOB_ID" | tr -c 'A-Za-z0-9_.-' '_')
STAGING="${OUTPUT}.__staging_${SAFE_JOB_ID}_$(date -u +%Y%m%dT%H%M%SZ)_$$"
export BDA_JOB_ID BDA_TASK_ID

cleanup_staging_on_exit() {
    local rc=$?
    if [ "$rc" -ne 0 ] && check_hdfs_path "$STAGING"; then
        log WARNING output_staging_cleanup "path=$STAGING result=failed_job_cleanup"
        run_hdfs output_staging_cleanup hdfs dfs -rm -r "$STAGING" || true
    fi
}
trap cleanup_staging_on_exit EXIT

echo "=== Hadoop Streaming MapReduce: $TITLE ==="
export HADOOP_CLASSPATH=/opt/hadoop-3.2.1/share/hadoop/mapreduce/*:/opt/hadoop-3.2.1/share/hadoop/mapreduce/lib/*
log INFO job.start "title=$TITLE output=$OUTPUT"

echo "[1/4] Ensuring raw ClarkNet log exists in HDFS..."
run_hdfs input_directories hdfs dfs -mkdir -p /user/data/raw_logs /user/data/results || exit $?
if check_hdfs_path /user/data/raw_logs/access_log; then
    log INFO input_validation "path=/user/data/raw_logs/access_log result=present"
else
    CHECK_RC=$?
    if [ "$CHECK_RC" -ne 1 ]; then
        log ERROR input_validation "path=/user/data/raw_logs/access_log result=unknown exit_code=$CHECK_RC action=refuse_to_modify_input"
        exit "$CHECK_RC"
    fi
    log WARNING input_validation "path=/user/data/raw_logs/access_log result=confirmed_missing"
    if [ -f /tmp/access_log ]; then
        run_hdfs input_upload hdfs dfs -put /tmp/access_log /user/data/raw_logs/access_log || exit $?
        check_hdfs_path /user/data/raw_logs/access_log || {
            CHECK_RC=$?
            log ERROR input_upload "path=/user/data/raw_logs/access_log result=verification_failed exit_code=$CHECK_RC"
            exit "$CHECK_RC"
        }
    else
        log ERROR input_validation "path=/user/data/raw_logs/access_log local_fallback=/tmp/access_log result=missing"
        exit 1
    fi
fi

echo "[2/4] Preparing isolated staging output: $STAGING"
if check_hdfs_path "$STAGING"; then
    log ERROR output_staging "path=$STAGING result=already_exists action=refuse_to_overwrite"
    exit 1
else
    CHECK_RC=$?
    if [ "$CHECK_RC" -ne 1 ]; then
        log ERROR output_staging "path=$STAGING result=existence_unknown exit_code=$CHECK_RC action=refuse_to_submit"
        exit "$CHECK_RC"
    fi
fi
log INFO output_staging "path=$STAGING result=ready"

if STREAMING_JAR=$(find /opt/hadoop*/share/hadoop/tools/lib/ -name "hadoop-streaming-*.jar" -print -quit); then
    :
else
    RC=$?
    log ERROR submission "operation=find_streaming_jar exit_code=$RC"
    exit "$RC"
fi
if [ -z "$STREAMING_JAR" ]; then
    log CRITICAL submission "operation=find_streaming_jar result=not_found"
    exit 1
fi

# Build the Streaming -file arguments for all referenced scripts.
# If mapper == reducer (e.g. reducer.py used by many tasks) dedup to avoid
# "file already added" errors that some Hadoop versions raise.
FILE_ARGS=(-file "/tmp/batch/mapreduce_job/streaming_runner.py"
    -file "/tmp/batch/mapreduce_job/$MAPPER")
if [ "$REDUCER" != "$MAPPER" ]; then
    FILE_ARGS+=(-file "/tmp/batch/mapreduce_job/$REDUCER")
fi

echo "[3/4] Running mapper=$MAPPER reducer=$REDUCER → staging=$STAGING"
log INFO submission "streaming_jar=$STREAMING_JAR mapper=$MAPPER reducer=$REDUCER output=$STAGING"
if hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="BDA-$TASK-$BDA_JOB_ID" \
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
    -output "$STAGING" \
    -cmdenv "BDA_JOB_ID=$BDA_JOB_ID" \
    -cmdenv "BDA_TASK_ID=$BDA_TASK_ID" \
    -mapper "python3 streaming_runner.py mapper $MAPPER" \
    -reducer "python3 streaming_runner.py reducer $REDUCER" \
    "${FILE_ARGS[@]}"; then
    :
else
    RC=$?
    log ERROR submission "operation=hadoop_streaming exit_code=$RC application_id=see_driver_log output=$STAGING"
    python3 /tmp/batch/mapreduce_job/publish_output.py \
        --task "$TASK" --final "$OUTPUT" --staging "$STAGING" \
        --job-id "$BDA_JOB_ID" --driver-exit-code "$RC" || true
    exit "$RC"
fi

echo "[4/4] Validating and publishing output: $OUTPUT"
if python3 /tmp/batch/mapreduce_job/publish_output.py \
    --task "$TASK" --final "$OUTPUT" --staging "$STAGING" --job-id "$BDA_JOB_ID"; then
    :
else
    RC=$?
    log ERROR output_publish "operation=publish_staged_output exit_code=$RC application_id=see_driver_log staging=$STAGING final=$OUTPUT"
    exit 1
fi

if ! run_hdfs output_summary hdfs dfs -count "$OUTPUT"; then
    log WARNING output_summary "operation=hdfs_dfs_count path=$OUTPUT result=summary_unavailable output_remains_published=true"
fi

log INFO job.completed "output=$OUTPUT result=success"
echo "=== Done ==="
