#!/usr/bin/env bash
# Destructively reset only this Compose project's NameNode and DataNode volumes.
# Run only after reviewing the warning and typing the explicit confirmation.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"; }

need docker
need python3

[[ -s "$ROOT/data/raw/access_log" ]] || die "Local dataset missing or empty: $ROOT/data/raw/access_log"

CONTEXT="$(docker context show)" || die "Cannot query Docker context"
ENDPOINT="$(docker context inspect "$CONTEXT" --format '{{.Endpoints.docker.Host}}')" || die "Cannot inspect Docker endpoint"
[[ "$ENDPOINT" == "unix:///var/run/docker.sock" ]] || die "Expected the WSL Docker socket, found context '$CONTEXT' at '$ENDPOINT'. Refusing to touch volumes."
docker info >/dev/null 2>&1 || die "Docker Engine is not reachable"

EXPECTED_PROJECT="$(basename "$ROOT")"
[[ "$EXPECTED_PROJECT" == "bdaproject" ]] || die "Unexpected project directory name '$EXPECTED_PROJECT'; expected bdaproject."

CONFIG_JSON="$(docker compose config --format json)" || die "Cannot resolve docker-compose.yml"
VOLUME_PLAN="$(python3 -c '
import json, sys

cfg = json.load(sys.stdin)
project = sys.argv[1]
expected = {
    "namenode": ("hadoop_namenode", "/hadoop/dfs/name"),
    "datanode": ("hadoop_datanode", "/hadoop/dfs/data"),
    "datanode2": ("hadoop_datanode2", "/hadoop/dfs/data"),
    "datanode3": ("hadoop_datanode3", "/hadoop/dfs/data"),
}
if cfg.get("name") != project:
    raise SystemExit(f"ambiguous Compose project name: {cfg.get('name')!r}")
resolved = {}
for service, (logical, target) in expected.items():
    mounts = cfg.get("services", {}).get(service, {}).get("volumes", [])
    volume_mounts = [m for m in mounts if m.get("type") == "volume"]
    if len(volume_mounts) != 1 or volume_mounts[0].get("source") != logical or volume_mounts[0].get("target") != target:
        raise SystemExit(f"ambiguous volume mapping for {service}: {mounts!r}")
    volume = cfg.get("volumes", {}).get(logical, {})
    name = volume.get("name")
    if name != f"{project}_{logical}" or volume.get("external", False):
        raise SystemExit(f"unexpected/external volume mapping for {logical}: {volume!r}")
    resolved[logical] = name

targets = set(resolved)
for service, service_cfg in cfg.get("services", {}).items():
    if service in expected:
        continue
    for mount in service_cfg.get("volumes", []):
        if mount.get("type") == "volume" and mount.get("source") in targets:
            raise SystemExit(f"HDFS volume {mount.get('source')!r} is also mounted by {service}; refusing")

other_names = {
    cfg.get("volumes", {}).get("hive_postgresql", {}).get("name"),
    cfg.get("volumes", {}).get("mongo_data", {}).get("name"),
}
if len(set(resolved.values())) != len(resolved) or set(resolved.values()) & other_names:
    raise SystemExit("HDFS volumes alias each other or a Hive/Mongo volume")
for logical in ("hadoop_namenode", "hadoop_datanode", "hadoop_datanode2", "hadoop_datanode3"):
    print(resolved[logical])
' "$EXPECTED_PROJECT" <<<"$CONFIG_JSON")" || die "Compose volume mapping is ambiguous; no volumes were changed."
mapfile -t TARGET_VOLUMES <<< "$VOLUME_PLAN"
[[ "${#TARGET_VOLUMES[@]}" -eq 4 ]] || die "Expected exactly four HDFS volumes; no volumes were changed."

# Do not reset while the app or any Compose container can still access HDFS.
if [[ -f "$ROOT/.run/api.pid" ]]; then
    API_PID="$(cat "$ROOT/.run/api.pid" 2>/dev/null || true)"
    if [[ "$API_PID" =~ ^[0-9]+$ ]] && kill -0 "$API_PID" 2>/dev/null; then
        die "API is still running (PID $API_PID). Stop the app with Ctrl+C or ./stop.sh first."
    fi
fi
COMPOSE_CONTAINERS="$(docker compose ps -a -q)" || die "Cannot inspect Compose containers"
[[ -z "$COMPOSE_CONTAINERS" ]] || die "Compose containers still exist. Stop the project with ./stop.sh first; it preserves named volumes."

for volume in "${TARGET_VOLUMES[@]}"; do
    docker volume inspect "$volume" >/dev/null 2>&1 || die "Expected HDFS volume '$volume' is absent; refusing reset."
    DRIVER="$(docker volume inspect "$volume" --format '{{.Driver}}')"
    [[ "$DRIVER" == "local" ]] || die "Unexpected driver '$DRIVER' for '$volume'; refusing reset."
    HOLDERS="$(docker ps -aq --filter "volume=$volume")" || die "Cannot check volume users"
    [[ -z "$HOLDERS" ]] || die "Volume '$volume' is still attached to a container; refusing reset."
done

echo
echo "DESTRUCTIVE HDFS RESET"
echo "Project: $EXPECTED_PROJECT"
echo "Docker context/endpoint: $CONTEXT / $ENDPOINT"
echo "The only volumes to be deleted are:"
for volume in "${TARGET_VOLUMES[@]}"; do
    docker volume inspect "$volume" --format '  {{.Name}} ({{.Mountpoint}})'
done
echo
echo "This permanently deletes the HDFS namespace and all HDFS blocks, including /user data, MapReduce outputs, and YARN recovery state under /rmstate."
echo "MongoDB and Hive PostgreSQL volumes and all project files are excluded. Back up any HDFS data you need before proceeding."
echo "The website/API must be stopped; run ./stop.sh first (it does not use -v)."
echo
read -r -p 'Type RESET HDFS STORAGE to delete exactly the four listed HDFS volumes: ' CONFIRM
[[ "$CONFIRM" == "RESET HDFS STORAGE" ]] || die "Confirmation did not match; no volumes were changed."

echo "Removing only the four confirmed HDFS volumes..."
docker volume rm "${TARGET_VOLUMES[@]}"

echo "Creating a fresh HDFS with the same Compose project..."
docker compose up -d namenode datanode datanode2 datanode3

echo "Waiting for all three DataNodes to register..."
DEADLINE=$((SECONDS + 240))
while (( SECONDS < DEADLINE )); do
    if docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -F 'Live datanodes (3):' >/dev/null; then
        break
    fi
    sleep 3
done
(( SECONDS < DEADLINE )) || die "Three DataNodes did not register within 240 seconds. Inspect docker compose logs."

echo "Waiting for HDFS safe mode to turn off..."
DEADLINE=$((SECONDS + 240))
while (( SECONDS < DEADLINE )); do
    SAFE_MODE="$(docker exec namenode hdfs dfsadmin -safemode get 2>&1 || true)"
    if grep -Fq 'Safe mode is OFF' <<< "$SAFE_MODE"; then
        break
    fi
    sleep 3
done
(( SECONDS < DEADLINE )) || die "HDFS did not leave safe mode within 240 seconds. Inspect NameNode and DataNode logs."

echo "Verifying an HDFS write/read cycle..."
CHECK_ID="reset-check-$$-$(date +%s)"
LOCAL_CHECK="$(mktemp)"
CONTAINER_CHECK="/tmp/$CHECK_ID"
HDFS_CHECK="/tmp/$CHECK_ID"
printf 'hdfs-reset-check:%s' "$CHECK_ID" > "$LOCAL_CHECK"
cleanup_check() {
    rm -f "$LOCAL_CHECK"
    docker exec namenode rm -f "$CONTAINER_CHECK" >/dev/null 2>&1 || true
    docker exec namenode hdfs dfs -rm -f "$HDFS_CHECK" >/dev/null 2>&1 || true
}
trap cleanup_check EXIT
docker cp "$LOCAL_CHECK" "namenode:$CONTAINER_CHECK" >/dev/null
docker exec namenode hdfs dfs -put "$CONTAINER_CHECK" "$HDFS_CHECK"
EXPECTED="$(cat "$LOCAL_CHECK")"
ACTUAL="$(docker exec namenode hdfs dfs -cat "$HDFS_CHECK")"
[[ "$ACTUAL" == "$EXPECTED" ]] || die "HDFS read-back did not match the test data."
cleanup_check
trap - EXIT

echo "Uploading the repository dataset to HDFS..."
docker cp "$ROOT/data/raw/access_log" namenode:/tmp/reset_access_log
docker exec namenode hdfs dfs -mkdir -p /user/data/raw_logs /user/data/results
docker exec namenode hdfs dfs -put /tmp/reset_access_log /user/data/raw_logs/access_log
docker exec namenode rm -f /tmp/reset_access_log
docker exec namenode hdfs dfs -test -e /user/data/raw_logs/access_log
docker exec namenode hdfs fsck /user/data/raw_logs/access_log -files -blocks -locations

echo "Starting ResourceManager and NodeManager..."
docker compose up -d resourcemanager nodemanager

echo "Waiting for ResourceManager health..."
DEADLINE=$((SECONDS + 240))
while (( SECONDS < DEADLINE )); do
    RM_HEALTH="$(docker inspect resourcemanager --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>/dev/null || true)"
    [[ "$RM_HEALTH" == "healthy" ]] && break
    sleep 3
done
[[ "${RM_HEALTH:-}" == "healthy" ]] || die "ResourceManager did not become healthy within 240 seconds. Inspect docker compose logs."

echo "Waiting for a running NodeManager registration..."
DEADLINE=$((SECONDS + 180))
while (( SECONDS < DEADLINE )); do
    NODES="$(docker exec namenode yarn node -list -all 2>&1 || true)"
    if grep -Eq 'Total Nodes:[[:space:]]*[1-9]' <<< "$NODES" && grep -q 'RUNNING' <<< "$NODES"; then
        break
    fi
    sleep 3
done
if ! grep -Eq 'Total Nodes:[[:space:]]*[1-9]' <<< "${NODES:-}" || ! grep -q 'RUNNING' <<< "${NODES:-}"; then
    die "No running NodeManager registered within 180 seconds. Inspect ResourceManager and NodeManager logs."
fi

echo "HDFS reset checks passed. Existing HDFS data and YARN recovery state were erased by explicit confirmation."
echo "Run ./start.sh to start the website and remaining project services."
