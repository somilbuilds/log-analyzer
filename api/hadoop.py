import os
import subprocess

TASK_OUTPUTS = {
    "status": "/user/data/results/status_counts",
    "hosts": "/user/data/results/top_hosts",
    "endpoints": "/user/data/results/top_endpoints",
}


def _run_docker_exec(container, cmd):
    try:
        result = subprocess.run(["docker", "exec", container] + cmd, capture_output=True, text=True, check=True)
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "output": e.stderr or e.stdout}


def _base_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _raw_file():
    return os.path.join(_base_dir(), "data", "raw", "access_log")


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


def get_hdfs_status():
    res = _run_docker_exec("namenode", ["hdfs", "dfs", "-ls", "-R", "/user/data"])
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
                "owner": parts[2],
                "size": parts[4],
                "date": parts[5] + " " + parts[6],
                "path": parts[7],
            })
    dataset_ready = any(f["path"] == "/user/data/raw_logs/access_log" for f in files)
    return {"available": True, "files": files, "dataset_ready": dataset_ready}


def get_mapreduce_results(task="status"):
    output = TASK_OUTPUTS.get(task, TASK_OUTPUTS["status"])
    res = _run_docker_exec("namenode", ["bash", "-lc", f"hdfs dfs -cat {output}/part-* 2>/dev/null"])
    if res["status"] == "error" or not res["output"].strip():
        return {"available": False, "task": task, "results": {}, "rows": []}

    results = {}
    rows = []
    total = 0
    for line in res["output"].strip().split("\n"):
        parts = line.split("\t")
        if len(parts) == 2:
            key = parts[0]
            count = int(parts[1])
            results[key] = count
            rows.append({"key": key, "count": count})
            total += count
    rows.sort(key=lambda row: row["count"], reverse=True)
    return {"available": True, "task": task, "results": results, "rows": rows[:50], "total": total}


def get_dataset_info():
    raw_file = _raw_file()
    if not os.path.exists(raw_file):
        return {"available": False, "size": 0, "lines": 0}

    size = os.path.getsize(raw_file)
    lines = 0
    try:
        res = subprocess.run(["wc", "-l", raw_file], capture_output=True, text=True)
        lines = int(res.stdout.split()[0])
    except Exception:
        pass

    return {"available": True, "size_bytes": size, "lines": lines}
