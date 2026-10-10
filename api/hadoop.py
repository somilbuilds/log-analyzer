"""
HDFS / MapReduce integration layer for ClarkNet Analytics Laboratory.
Provides: HDFS status, diagnostics (report, fsck), dataset info,
MapReduce 25-task catalog, job submission, and result parsing.
"""

import os
import subprocess
import re

# ── 25-Task Analytical Catalog ───────────────────────────────────

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
    {"id": "method_status","name": "Method × Status Cross-Tab",   "category": "Responses & Errors", "mapper": "method_status_mapper.py", "reducer": "reducer.py",     "output": "/user/data/results/method_status",   "chart": "stacked","description": "HTTP method versus status code cross-tabulation"},
    {"id": "error_rate_day","name": "Error Rate Over Time",       "category": "Responses & Errors", "mapper": "error_rate_day_mapper.py","reducer": "reducer.py",     "output": "/user/data/results/error_rate_day",  "chart": "line",   "description": "Daily error rates (total vs error requests by day)"},

    # Category 3 — Traffic characteristics
    {"id": "bytes_dist",  "name": "Response Size Distribution",   "category": "Traffic",            "mapper": "bytes_mapper.py",         "reducer": "reducer.py",     "output": "/user/data/results/bytes_dist",      "chart": "histogram","description": "Histogram of response sizes by bucket"},
    {"id": "bytes_path",  "name": "Bandwidth by Path",            "category": "Traffic",            "mapper": "bytes_total_mapper.py",   "reducer": "sum_reducer.py", "output": "/user/data/results/bytes_path",      "chart": "bar",    "description": "Total bytes transferred per URL path"},
    {"id": "bytes_host",  "name": "Traffic Volume by Client",     "category": "Traffic",            "mapper": "host_bytes_mapper.py",    "reducer": "sum_reducer.py", "output": "/user/data/results/bytes_host",      "chart": "bar",    "description": "Total bytes transferred per client IP"},

    # Category 4 — Behavioral and security-oriented
    {"id": "high_rate_hosts",  "name": "High-Rate Clients",        "category": "Security",          "mapper": "host_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/top_hosts",       "chart": "bar",    "description": "Clients with highest request rates (indicator only)"},
    {"id": "failed_requests",  "name": "Repeated Failed Requests", "category": "Security",          "mapper": "error_host_mapper.py",    "reducer": "reducer.py",     "output": "/user/data/results/error_hosts",     "chart": "bar",    "description": "Clients with repeated 4xx/5xx responses (potential probing)"},
    {"id": "unusual_paths",    "name": "Rare Paths",               "category": "Security",          "mapper": "endpoint_mapper.py",      "reducer": "reducer.py",     "output": "/user/data/results/top_endpoints",   "chart": "table",  "description": "Least common requested paths (statistical outliers)"},
    {"id": "suspicious_combos","name": "Suspicious Method+Status", "category": "Security",          "mapper": "method_status_mapper.py", "reducer": "reducer.py",     "output": "/user/data/results/method_status",   "chart": "table",  "description": "Unusual HTTP method and status combinations"},
    {"id": "rare_status",      "name": "Rare Status Codes",        "category": "Security",          "mapper": "status_mapper.py",        "reducer": "reducer.py",     "output": "/user/data/results/status_counts",   "chart": "table",  "description": "Least-common HTTP status codes"},

    # Additional analyses
    {"id": "volume_time",      "name": "Request Volume Over Time", "category": "Hosts & Volume",     "mapper": "day_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/daily",           "chart": "area",   "description": "Request volume time series by day"},
    {"id": "host_concentration","name": "Client Concentration",    "category": "Traffic",            "mapper": "host_mapper.py",          "reducer": "reducer.py",     "output": "/user/data/results/top_hosts",       "chart": "pareto", "description": "Traffic share analysis — what fraction of clients produce what fraction of traffic"},
    {"id": "path_popularity",  "name": "Path Popularity Time",     "category": "Traffic",            "mapper": "endpoint_mapper.py",      "reducer": "reducer.py",     "output": "/user/data/results/top_endpoints",   "chart": "treemap","description": "Resource popularity ranked view"},
    {"id": "malformed",        "name": "Malformed Records",        "category": "Traffic",            "mapper": "status_mapper.py",        "reducer": "reducer.py",     "output": "/user/data/results/status_counts",   "chart": "table",  "description": "Records that failed CLF parsing (estimated from status=0)"},
]

# Build lookup from id → task config
_TASK_LOOKUP = {t["id"]: t for t in TASK_CATALOG}


def _run_docker_exec(container, cmd, timeout=10):
    try:
        result = subprocess.run(
            ["docker", "exec", container] + cmd,
            capture_output=True, text=True, check=True, timeout=timeout,
        )
        return {"status": "success", "output": result.stdout}
    except subprocess.TimeoutExpired:
        return {"status": "error", "output": "Command timed out"}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "output": e.stderr or e.stdout}
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

    subprocess.run(["docker", "cp", raw_file, "namenode:/tmp/access_log"], check=False)
    res = _run_docker_exec("namenode", [
        "bash", "-lc",
        "hdfs dfs -mkdir -p /user/data/raw_logs && "
        "hdfs dfs -put -f /tmp/access_log /user/data/raw_logs/access_log && "
        "hdfs dfs -ls /user/data/raw_logs/access_log"
    ])
    return {
        "available": res["status"] == "success",
        "uploaded": res["status"] == "success",
        "message": res["output"],
        "path": "/user/data/raw_logs/access_log",
    }


# ── HDFS Status ──────────────────────────────────────────────────

def get_hdfs_status(timeout=4):
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
    return {"available": True, "files": files, "dataset_ready": dataset_ready}


# ── HDFS Diagnostics ─────────────────────────────────────────────

def get_hdfs_report():
    """Get HDFS dfsadmin report — DataNode capacities, block counts, etc."""
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

    return report


def get_hdfs_fsck():
    """Run hdfs fsck on the dataset path."""
    res = _run_docker_exec("namenode", [
        "hdfs", "fsck", "/user/data/raw_logs/access_log", "-files", "-blocks"
    ])
    if res["status"] == "error":
        return {"available": False, "message": "HDFS fsck unavailable", "raw": ""}

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

    return fsck


# ── MapReduce Results ────────────────────────────────────────────

def get_mapreduce_results(task="status"):
    task_config = _TASK_LOOKUP.get(task)
    if not task_config:
        return {"available": False, "task": task, "results": {}, "rows": [], "error": "Unknown task"}

    output_path = task_config["output"]
    res = _run_docker_exec("namenode", ["bash", "-lc", f"hdfs dfs -cat {output_path}/part-* 2>/dev/null"])
    if res["status"] == "error" or not res["output"].strip():
        return {"available": False, "task": task, "results": {}, "rows": [],
                "task_name": task_config["name"], "chart_type": task_config["chart"]}

    results = {}
    rows = []
    total = 0
    for line in res["output"].strip().split("\n"):
        parts = line.split("\t")
        if len(parts) == 2:
            key = parts[0]
            try:
                count = int(parts[1])
            except ValueError:
                continue
            results[key] = count
            rows.append({"key": key, "count": count})
            total += count
    rows.sort(key=lambda row: row["count"], reverse=True)

    return {
        "available": True,
        "task": task,
        "task_name": task_config["name"],
        "category": task_config["category"],
        "chart_type": task_config["chart"],
        "description": task_config["description"],
        "results": results,
        "rows": rows[:100],
        "total": total,
        "total_keys": len(rows),
    }


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
