"""
HDFS / MapReduce integration layer for ClarkNet Analytics Laboratory.
Provides: HDFS status, diagnostics (report, fsck), dataset info,
MapReduce 25-task catalog, job submission, and result parsing.
"""

import os
import subprocess
import re
import time

_HDFS_STATUS_CACHE = {"data": None, "ts": 0}
_HDFS_REPORT_CACHE = {"data": None, "ts": 0}
_HDFS_FSCK_CACHE = {"data": None, "ts": 0}

# ── 25-Task Analytical Catalog ───────────────────────────────────
# Every task has a UNIQUE output directory to prevent cross-task overwrites.

TASK_CATALOG = [
    # Category 1 — Hosts and request volume
    {"id": "hosts",       "name": "Top Client IPs",              "category": "Hosts & Volume",     "mapper": "host_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/top_hosts",       "chart": "bar",    "description": "Rank client IPs by request count"},
    {"id": "endpoints",   "name": "Top Requested Paths",         "category": "Hosts & Volume",     "mapper": "endpoint_mapper.py",      "reducer": "reducer.py",     "output": "/user/data/results/top_endpoints",   "chart": "bar",    "description": "Most frequently requested URL paths"},
    {"id": "extensions",  "name": "File Extension Distribution",  "category": "Hosts & Volume",     "mapper": "extension_mapper.py",     "reducer": "reducer.py",     "output": "/user/data/results/extensions",      "chart": "bar",    "description": "Distribution of requested file types"},
    {"id": "methods",     "name": "HTTP Method Distribution",     "category": "Hosts & Volume",     "mapper": "method_mapper.py",        "reducer": "reducer.py",     "output": "/user/data/results/methods",         "chart": "pie",    "description": "Breakdown by HTTP method (GET, POST, HEAD, etc.)"},
    {"id": "hourly",      "name": "Requests by Hour",             "category": "Hosts & Volume",     "mapper": "hour_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/hourly",          "chart": "line",   "description": "Request volume by hour of day (0–23)"},
    {"id": "daily",       "name": "Requests by Day",              "category": "Hosts & Volume",     "mapper": "day_mapper.py",           "reducer": "reducer.py",     "output": "/user/data/results/daily",           "chart": "line",   "description": "Request volume by calendar day"},
    {"id": "hour_day",    "name": "Day×Hour Heatmap",             "category": "Hosts & Volume",     "mapper": "hour_day_mapper.py",      "reducer": "reducer.py",     "output": "/user/data/results/hour_day",        "chart": "heatmap","description": "Day-of-week × hour-of-day traffic heatmap"},

    # Category 2 — HTTP responses and errors
    {"id": "status",      "name": "Status Code Distribution",     "category": "Responses & Errors", "mapper": "status_mapper.py",        "reducer": "reducer.py",     "output": "/user/data/results/status_counts",   "chart": "bar",    "description": "Distribution of all HTTP status codes"},
    {"id": "status_4xx",  "name": "4xx Client Errors",            "category": "Responses & Errors", "mapper": "status4xx_mapper.py",     "reducer": "reducer.py",     "output": "/user/data/results/status_4xx",      "chart": "bar",    "description": "Breakdown of 4xx client error codes"},
    {"id": "status_5xx",  "name": "5xx Server Errors",            "category": "Responses & Errors", "mapper": "status5xx_mapper.py",     "reducer": "reducer.py",     "output": "/user/data/results/status_5xx",      "chart": "bar",    "description": "Breakdown of 5xx server error codes"},
    {"id": "error_paths", "name": "Error-Producing Paths",        "category": "Responses & Errors", "mapper": "error_path_mapper.py",    "reducer": "reducer.py",     "output": "/user/data/results/error_paths",     "chart": "bar",    "description": "URL paths that produced the most 4xx/5xx errors"},
    {"id": "error_hosts", "name": "High-Error Clients",           "category": "Responses & Errors", "mapper": "error_host_mapper.py",    "reducer": "reducer.py",     "output": "/user/data/results/error_hosts",     "chart": "bar",    "description": "Client IPs with the highest 4xx/5xx error counts"},
    {"id": "method_status","name": "Method × Status Cross-Tab",   "category": "Responses & Errors", "mapper": "method_status_mapper.py", "reducer": "method_status_reducer.py", "output": "/user/data/results/method_status",   "chart": "stacked","description": "HTTP method versus status code cross-tabulation"},
    {"id": "error_rate_day","name": "Error Rate Over Time",       "category": "Responses & Errors", "mapper": "error_rate_day_mapper.py","reducer": "error_rate_day_reducer.py", "output": "/user/data/results/error_rate_day",  "chart": "line",   "description": "Daily error rates (total vs error requests by day)"},

    # Category 3 — Traffic characteristics
    {"id": "bytes_dist",  "name": "Response Size Distribution",   "category": "Traffic",            "mapper": "bytes_mapper.py",         "reducer": "reducer.py",     "output": "/user/data/results/bytes_dist",      "chart": "histogram","description": "Histogram of response sizes by bucket"},
    {"id": "bytes_path",  "name": "Bandwidth by Path",            "category": "Traffic",            "mapper": "bytes_total_mapper.py",   "reducer": "sum_reducer.py", "output": "/user/data/results/bytes_path",      "chart": "bar",    "description": "Total bytes transferred per URL path"},
    {"id": "bytes_host",  "name": "Traffic Volume by Client",     "category": "Traffic",            "mapper": "host_bytes_mapper.py",    "reducer": "sum_reducer.py", "output": "/user/data/results/bytes_host",      "chart": "bar",    "description": "Total bytes transferred per client IP"},

    # Category 4 — Behavioral and security-oriented (unique output dirs)
    {"id": "high_rate_hosts",  "name": "High-Rate Clients",        "category": "Security",          "mapper": "host_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/high_rate_hosts",       "chart": "bar",    "description": "Clients with highest request counts (potential high-rate)"},
    {"id": "failed_requests",  "name": "Repeated Failed Requests", "category": "Security",          "mapper": "error_host_mapper.py",    "reducer": "reducer.py",     "output": "/user/data/results/failed_requests",     "chart": "bar",    "description": "Clients with repeated 4xx/5xx responses (potential probing)"},
    {"id": "unusual_paths",    "name": "Rare Paths",               "category": "Security",          "mapper": "endpoint_mapper.py",      "reducer": "reducer.py",     "output": "/user/data/results/unusual_paths",   "chart": "table",  "description": "Least common requested paths (statistical outliers)"},
    {"id": "suspicious_combos","name": "Suspicious Method+Status", "category": "Security",          "mapper": "method_status_mapper.py", "reducer": "method_status_reducer.py", "output": "/user/data/results/suspicious_combos",   "chart": "table",  "description": "Unusual HTTP method and status combinations"},
    {"id": "rare_status",      "name": "Rare Status Codes",        "category": "Security",          "mapper": "status_mapper.py",        "reducer": "reducer.py",     "output": "/user/data/results/rare_status",     "chart": "table",  "description": "Least-common HTTP status codes"},

    # Additional analyses (unique output dirs)
    {"id": "volume_time",      "name": "Request Volume Over Time", "category": "Hosts & Volume",     "mapper": "day_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/volume_time",     "chart": "area",   "description": "Request volume time series by day"},
    {"id": "host_concentration","name": "Client Concentration",    "category": "Traffic",            "mapper": "host_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/host_concentration", "chart": "pareto", "description": "Traffic share analysis — what fraction of clients produce what fraction of traffic"},
    {"id": "path_popularity",  "name": "Path Popularity",          "category": "Traffic",            "mapper": "endpoint_mapper.py",      "reducer": "reducer.py",     "output": "/user/data/results/path_popularity", "chart": "treemap","description": "Resource popularity ranked view"},
    {"id": "malformed",        "name": "Malformed Records",        "category": "Traffic",            "mapper": "malformed_mapper.py",     "reducer": "reducer.py",     "output": "/user/data/results/malformed",       "chart": "table",  "description": "Records that failed CLF parsing"},
]

# Build lookup from id → task config
_TASK_LOOKUP = {t["id"]: t for t in TASK_CATALOG}

# Tasks that need ascending sort (rarest first)
_ASCENDING_SORT_TASKS = {"rare_status", "unusual_paths"}

# Tasks that need chronological sort (by key, not count)
_CHRONOLOGICAL_SORT_TASKS = {"hourly", "daily", "volume_time", "error_rate_day"}

# Tasks with multi-column output (METHOD\tSTATUS\tCOUNT)
_MULTI_COL_TASKS = {"method_status", "suspicious_combos"}

# Tasks with error_rate_day format (DAY\tTOTAL\tERROR\tPCT)
_ERROR_RATE_TASKS = {"error_rate_day"}


# Maximum bytes to read from HDFS result output (10 MB)
_MAX_RESULT_BYTES = 10 * 1024 * 1024

# Lines that are Hadoop diagnostic noise — not actual result rows
_NOISE_PREFIXES = (
    "WARNING:", "INFO:", "DEBUG:", "ERROR:",
    "log4j:", "SLF4J:",
    "Connecting to", "DEPRECATED:",
    "  ",  # indented continuation lines from diagnostic output
)


def _is_noise_line(line):
    """Return True if `line` is Hadoop diagnostic output, not a result row."""
    stripped = line.lstrip()
    if not stripped:
        return True
    # If the line contains a tab, it's likely a valid result row (key\tvalue)
    if '\t' in line:
        return False
    # Lines matching known noise prefixes
    for prefix in _NOISE_PREFIXES:
        if stripped.startswith(prefix):
            return True
    # Java-style log lines: "2024-01-01 ...", or "24/01/01 ..."
    # These are yyyy-mm-dd or yy/mm/dd timestamps from Hadoop/Java logging
    if len(stripped) > 10 and (stripped[4] == '-' or stripped[2] == '/'):
        parts = stripped.split(None, 2)
        if len(parts) >= 2 and (':' in parts[1]):
            return True
    return False


def _run_docker_exec(container, cmd, timeout=10):
    try:
        result = subprocess.run(
            ["docker", "exec", container] + cmd,
            capture_output=True, text=False, check=True, timeout=timeout,
        )
        # Decode as UTF-8 with replacement to handle non-UTF-8 bytes
        stdout = result.stdout.decode('utf-8', errors='replace')
        return {"status": "success", "output": stdout}
    except subprocess.TimeoutExpired:
        return {"status": "error", "output": "Command timed out"}
    except subprocess.CalledProcessError as e:
        stderr_text = (e.stderr or b'').decode('utf-8', errors='replace') if isinstance(e.stderr, bytes) else (e.stderr or '')
        stdout_text = (e.stdout or b'').decode('utf-8', errors='replace') if isinstance(e.stdout, bytes) else (e.stdout or '')
        return {"status": "error", "output": stderr_text or stdout_text or "Command failed"}
    except Exception as e:
        return {"status": "error", "output": str(e)}


def _base_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _raw_file():
    return os.path.join(_base_dir(), "data", "raw", "access_log")


# ── HDFS Dataset ─────────────────────────────────────────────────

def ensure_hdfs_dataset():
    raw_file = _raw_file()
    if not os.path.exists(raw_file):
        return {"available": False, "uploaded": False, "message": "local ClarkNet access_log missing"}

    # Check if dataset is already in HDFS to avoid expensive re-upload
    check = _run_docker_exec("namenode", ["hdfs", "dfs", "-test", "-e", "/user/data/raw_logs/access_log"], timeout=5)
    if check["status"] == "success":
        return {
            "available": True,
            "uploaded": True,
            "message": "Dataset present in HDFS",
            "path": "/user/data/raw_logs/access_log",
        }

    # Upload if missing
    subprocess.run(["docker", "cp", raw_file, "namenode:/tmp/access_log"], check=False)
    res = _run_docker_exec("namenode", [
        "bash", "-c",
        "hdfs dfs -mkdir -p /user/data/raw_logs && "
        "hdfs dfs -put -f /tmp/access_log /user/data/raw_logs/access_log && "
        "hdfs dfs -ls /user/data/raw_logs/access_log"
    ], timeout=90)
    return {
        "available": res["status"] == "success",
        "uploaded": res["status"] == "success",
        "message": res["output"],
        "path": "/user/data/raw_logs/access_log",
    }


# ── HDFS Status ──────────────────────────────────────────────────

def get_hdfs_status(timeout=10):
    global _HDFS_STATUS_CACHE
    now = time.time()
    if _HDFS_STATUS_CACHE["data"] and (now - _HDFS_STATUS_CACHE["ts"]) < 8:
        return _HDFS_STATUS_CACHE["data"]

    res = _run_docker_exec("namenode", ["hdfs", "dfs", "-ls", "-R", "/user/data"], timeout=timeout)
    if res["status"] == "error":
        return {"available": False, "message": "HDFS unavailable", "files": [], "dataset_ready": False}

    files = []
    for line in res["output"].strip().split("\n"):
        if line.startswith("Found") or not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 8:
            files.append({
                "permissions": parts[0],
                "replication": parts[1],
                "owner": parts[2],
                "group": parts[3],
                "size": parts[4],
                "date": parts[5] + " " + parts[6],
                "path": parts[7],
                "is_dir": parts[0].startswith("d"),
            })
    dataset_ready = any(f["path"] == "/user/data/raw_logs/access_log" for f in files)
    result = {"available": True, "files": files, "dataset_ready": dataset_ready}
    _HDFS_STATUS_CACHE["data"] = result
    _HDFS_STATUS_CACHE["ts"] = now
    return result


# ── HDFS Diagnostics ─────────────────────────────────────────────

def get_hdfs_report():
    """Get HDFS dfsadmin report — DataNode capacities, block counts, etc."""
    global _HDFS_REPORT_CACHE
    now = time.time()
    if _HDFS_REPORT_CACHE["data"] and (now - _HDFS_REPORT_CACHE["ts"]) < 8:
        return _HDFS_REPORT_CACHE["data"]

    res = _run_docker_exec("namenode", ["hdfs", "dfsadmin", "-report"])
    if res["status"] == "error":
        return {"available": False, "message": "HDFS report unavailable", "raw": ""}

    output = res["output"]
    report = {"available": True, "raw": output, "nodes": [], "summary": {}}

    # Parse summary
    for pattern, key in [
        (r"Configured Capacity:\s+(\d+)", "configured_capacity"),
        (r"Present Capacity:\s+(\d+)", "present_capacity"),
        (r"DFS Remaining:\s+(\d+)", "dfs_remaining"),
        (r"DFS Used:\s+(\d+)", "dfs_used"),
        (r"DFS Used%:\s+([\d.]+)%", "dfs_used_pct"),
        (r"Replicated Blocks:\s*\n.*?Total size:\s+(\d+)", "total_block_size"),
        (r"Live datanodes\s*\((\d+)\)", "live_datanodes"),
        (r"Dead datanodes\s*\((\d+)\)", "dead_datanodes"),
    ]:
        m = re.search(pattern, output, re.MULTILINE)
        if m:
            val = m.group(1)
            report["summary"][key] = float(val) if "." in val else int(val)

    # Parse datanode blocks
    node_blocks = output.split("Name:")
    for block in node_blocks[1:]:
        node = {}
        for pattern, key in [
            (r"^([\d.:]+)", "address"),
            (r"Hostname:\s+(\S+)", "hostname"),
            (r"Configured Capacity:\s+(\d+)", "capacity"),
            (r"DFS Used:\s+(\d+)", "used"),
            (r"DFS Remaining:\s+(\d+)", "remaining"),
            (r"DFS Used%:\s+([\d.]+)%", "used_pct"),
            (r"Num of Blocks:\s+(\d+)", "num_blocks"),
        ]:
            m = re.search(pattern, block, re.MULTILINE)
            if m:
                val = m.group(1)
                node[key] = float(val) if "." in val else (int(val) if val.isdigit() else val)
        if node:
            report["nodes"].append(node)

    # Add friendly names to datanodes
    for i, node in enumerate(report["nodes"], start=1):
        node["name"] = f"DataNode {i}"
        node["status"] = "LIVE"

    report["namenode"] = {
        "name": "NameNode (Master)",
        "host": "namenode",
        "rpc_port": 9000,
        "web_port": 9870,
        "status": "ACTIVE",
        "live_datanodes": report["summary"].get("live_datanodes", len(report["nodes"])),
        "total_capacity": report["summary"].get("configured_capacity", 0),
        "dfs_used": report["summary"].get("dfs_used", 0),
        "dfs_remaining": report["summary"].get("dfs_remaining", 0),
        "dfs_used_pct": report["summary"].get("dfs_used_pct", 0),
    }

    _HDFS_REPORT_CACHE["data"] = report
    _HDFS_REPORT_CACHE["ts"] = now
    return report


def get_hdfs_fsck():
    """Run hdfs fsck on the dataset path with block locations and sizes."""
    global _HDFS_FSCK_CACHE
    now = time.time()
    if _HDFS_FSCK_CACHE["data"] and (now - _HDFS_FSCK_CACHE["ts"]) < 10:
        return _HDFS_FSCK_CACHE["data"]

    res = _run_docker_exec("namenode", [
        "hdfs", "fsck", "/user/data/raw_logs/access_log", "-files", "-blocks", "-locations"
    ])
    if res["status"] == "error":
        return {"available": False, "message": "HDFS fsck unavailable", "raw": "", "blocks": []}

    output = res["output"]
    fsck = {"available": True, "raw": output, "blocks": []}

    # Parse block info
    for pattern, key in [
        (r"Total size:\s+(\d+)", "total_size"),
        (r"Total files:\s+(\d+)", "total_files"),
        (r"Total blocks.*?:\s+(\d+)", "total_blocks"),
        (r"Minimally replicated blocks:\s+(\d+)", "min_replicated"),
        (r"Over-replicated blocks:\s+(\d+)", "over_replicated"),
        (r"Under-replicated blocks:\s+(\d+)", "under_replicated"),
        (r"Default replication factor:\s+(\d+)", "replication_factor"),
        (r"Average block replication:\s+([\d.]+)", "avg_replication"),
    ]:
        m = re.search(pattern, output)
        if m:
            val = m.group(1)
            fsck[key] = float(val) if "." in val else int(val)

    # Parse individual block details and locations
    block_pattern = re.compile(r'(\d+)\.\s+.*?(blk_\d+_\d+)\s+len=(\d+)\s+Live_repl=(\d+)\s+\[(.*)\]')
    for m in block_pattern.finditer(output):
        idx, blk_id, length, repl, locs = m.groups()
        node_ips = re.findall(r'(\d+\.\d+\.\d+\.\d+:\d+)', locs)
        fsck["blocks"].append({
            "index": int(idx),
            "block_id": blk_id,
            "size_bytes": int(length),
            "replicas": int(repl),
            "locations": node_ips,
        })

    _HDFS_FSCK_CACHE["data"] = fsck
    _HDFS_FSCK_CACHE["ts"] = now
    return fsck


# ── MapReduce Results ────────────────────────────────────────────

def _parse_date_key(key):
    """Try to parse DD/Mon/YYYY into a sortable tuple for chronological ordering."""
    import calendar
    months = {v: k for k, v in enumerate(calendar.month_abbr) if v}
    try:
        parts = key.split("/")
        if len(parts) == 3:
            day, mon, year = parts
            return (int(year), months.get(mon, 0), int(day))
    except (ValueError, IndexError):
        pass
    return (9999, 99, 99)  # sort unknowns last


def get_mapreduce_results(task="status"):
    task_config = _TASK_LOOKUP.get(task)
    if not task_config:
        return {"available": False, "task": task, "results": {}, "rows": [], "error": "Unknown task"}

    output_path = task_config["output"]

    # Use a generous timeout (60s) to handle large result files across
    # the Docker+HDFS stack without false "Command timed out" errors.
    # Redirect stderr to /dev/null so Hadoop diagnostic messages don't
    # contaminate the result stream.
    res = _run_docker_exec(
        "namenode",
        ["bash", "-c", f"hdfs dfs -cat {output_path}/part-* 2>/dev/null"],
        timeout=60,
    )

    if res["status"] == "error":
        return {"available": False, "task": task, "results": {}, "rows": [],
                "task_name": task_config["name"], "chart_type": task_config["chart"],
                "error": "HDFS read failed: " + res.get("output", "unknown error")}

    raw_output = res["output"]

    # Guard against gigantic outputs: truncate to _MAX_RESULT_BYTES
    if len(raw_output) > _MAX_RESULT_BYTES:
        # Truncate to last complete line within the limit
        raw_output = raw_output[:_MAX_RESULT_BYTES]
        last_nl = raw_output.rfind('\n')
        if last_nl > 0:
            raw_output = raw_output[:last_nl]

    # Strip Hadoop diagnostic noise lines before parsing
    clean_lines = [line for line in raw_output.split('\n') if not _is_noise_line(line)]
    raw_output = '\n'.join(clean_lines).strip()

    if not raw_output:
        return {"available": False, "task": task, "results": {}, "rows": [],
                "task_name": task_config["name"], "chart_type": task_config["chart"],
                "error": "No output data found in HDFS"}

    # ── Task-specific parsing ────────────────────────────────────

    # error_rate_day: DAY\tTOTAL\tERROR\tPCT
    if task in _ERROR_RATE_TASKS:
        return _parse_error_rate_results(task, task_config, raw_output)

    # method_status / suspicious_combos: METHOD\tSTATUS\tCOUNT
    if task in _MULTI_COL_TASKS:
        return _parse_multi_col_results(task, task_config, raw_output)

    # Standard key\tcount parsing
    return _parse_standard_results(task, task_config, raw_output)


def _parse_error_rate_results(task, task_config, raw_output):
    """Parse error_rate_day output: DAY\tTOTAL\tERROR\tPCT"""
    rows = []
    for line in raw_output.split("\n"):
        parts = line.split("\t")
        if len(parts) >= 4:
            try:
                day = parts[0]
                total = int(parts[1])
                error = int(parts[2])
                pct = float(parts[3])
                rows.append({"key": day, "total": total, "error": error, "error_pct": pct, "count": error})
            except (ValueError, IndexError):
                continue
        elif len(parts) == 2:
            # Fallback for legacy key\tcount format
            try:
                rows.append({"key": parts[0], "count": int(parts[1])})
            except ValueError:
                continue

    # Chronological sort by date
    rows.sort(key=lambda r: _parse_date_key(r["key"]))

    total_requests = sum(r.get("total", r.get("count", 0)) for r in rows)
    total_errors = sum(r.get("error", 0) for r in rows)

    return {
        "available": True,
        "task": task,
        "task_name": task_config["name"],
        "category": task_config["category"],
        "chart_type": task_config["chart"],
        "description": task_config["description"],
        "results": {r["key"]: r.get("error_pct", r.get("count", 0)) for r in rows},
        "rows": rows,
        "total": total_requests,
        "total_errors": total_errors,
        "total_keys": len(rows),
    }


def _parse_multi_col_results(task, task_config, raw_output):
    """Parse method_status output: METHOD\tSTATUS\tCOUNT"""
    rows = []
    results = {}
    total = 0
    methods_set = set()
    statuses_set = set()

    for line in raw_output.split("\n"):
        parts = line.split("\t")
        if len(parts) >= 3:
            try:
                method = parts[0]
                status = parts[1]
                count = int(parts[2])
                key = f"{method}:{status}"
                rows.append({"key": key, "method": method, "status": status, "count": count})
                results[key] = count
                total += count
                methods_set.add(method)
                statuses_set.add(status)
            except (ValueError, IndexError):
                continue
        elif len(parts) == 2:
            # Fallback: composite key\tcount
            try:
                key = parts[0]
                count = int(parts[1])
                if ":" in key:
                    method, status = key.split(":", 1)
                    rows.append({"key": key, "method": method, "status": status, "count": count})
                    methods_set.add(method)
                    statuses_set.add(status)
                else:
                    rows.append({"key": key, "count": count})
                results[key] = count
                total += count
            except ValueError:
                continue

    # Sort by count descending for display, ascending for rare/suspicious
    if task in _ASCENDING_SORT_TASKS or task == "suspicious_combos":
        rows.sort(key=lambda r: r["count"])
    else:
        rows.sort(key=lambda r: r["count"], reverse=True)

    return {
        "available": True,
        "task": task,
        "task_name": task_config["name"],
        "category": task_config["category"],
        "chart_type": task_config["chart"],
        "description": task_config["description"],
        "results": results,
        "rows": rows[:200],
        "total": total,
        "total_keys": len(rows),
        "methods": sorted(methods_set),
        "statuses": sorted(statuses_set),
    }


def _parse_standard_results(task, task_config, raw_output):
    """Parse standard key\tcount output with task-specific sorting."""
    results = {}
    rows = []
    total = 0
    parse_errors = 0

    for line in raw_output.split("\n"):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 2:
            key = parts[0]
            try:
                count = int(parts[1])
            except ValueError:
                parse_errors += 1
                continue
            results[key] = count
            rows.append({"key": key, "count": count})
            total += count
        else:
            parse_errors += 1

    # Task-specific sorting
    if task in _ASCENDING_SORT_TASKS:
        rows.sort(key=lambda r: r["count"])
    elif task in _CHRONOLOGICAL_SORT_TASKS:
        if task == "hourly":
            # Sort by hour numerically: "00", "01", ..., "23"
            rows.sort(key=lambda r: r["key"])
        elif task in ("daily", "volume_time"):
            # Sort by date chronologically
            rows.sort(key=lambda r: _parse_date_key(r["key"]))
        else:
            rows.sort(key=lambda r: r["key"])
    else:
        # Default: descending by count
        rows.sort(key=lambda r: r["count"], reverse=True)

    response = {
        "available": True,
        "task": task,
        "task_name": task_config["name"],
        "category": task_config["category"],
        "chart_type": task_config["chart"],
        "description": task_config["description"],
        "results": results,
        "rows": rows[:200],
        "total": total,
        "total_keys": len(rows),
    }

    if parse_errors > 0:
        response["parse_errors"] = parse_errors

    return response


# ── Dataset Info ─────────────────────────────────────────────────

def get_dataset_info():
    raw_file = _raw_file()
    if not os.path.exists(raw_file):
        return {"available": False, "size_bytes": 0, "lines": 0}

    size = os.path.getsize(raw_file)
    lines = 0
    try:
        res = subprocess.run(["wc", "-l", raw_file], capture_output=True, text=True)
        lines = int(res.stdout.split()[0])
    except Exception:
        pass

    return {"available": True, "size_bytes": size, "lines": lines}
