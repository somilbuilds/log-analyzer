import os
import subprocess
import json

def _run_docker_exec(container, cmd):
    try:
        result = subprocess.run(["docker", "exec", container] + cmd, capture_output=True, text=True, check=True)
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "output": e.stderr}

def get_hdfs_status():
    res = _run_docker_exec("namenode", ["hdfs", "dfs", "-ls", "/user/data"])
    if res["status"] == "error":
        return {"available": False, "message": "HDFS unavailable", "files": []}
    
    files = []
    lines = res["output"].strip().split("\n")
    for line in lines:
        if line.startswith("Found"):
            continue
        parts = line.split()
        if len(parts) >= 8:
            files.append({
                "permissions": parts[0],
                "owner": parts[2],
                "size": parts[4],
                "date": parts[5] + " " + parts[6],
                "path": parts[7]
            })
    return {"available": True, "files": files}

def get_mapreduce_results():
    res = _run_docker_exec("namenode", ["hdfs", "dfs", "-cat", "/user/data/status_counts/part-*"])
    if res["status"] == "error":
        return {"available": False, "results": {}}
    
    results = {}
    total = 0
    for line in res["output"].strip().split("\n"):
        parts = line.split("\t")
        if len(parts) == 2:
            code = parts[0]
            count = int(parts[1])
            results[code] = count
            total += count
            
    return {"available": True, "results": results, "total": total}

def run_mapreduce_job():
    # Start it asynchronously? No, start.sh uses bash script. We can run it in a separate thread or just block?
    # Better to just use subprocess.Popen
    try:
        proc = subprocess.Popen(["docker", "exec", "namenode", "bash", "/data/batch/run_mapreduce.sh"])
        return {"status": "started", "pid": proc.pid}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

def get_dataset_info():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_file = os.path.join(base_dir, "data", "raw", "access_log")
    
    if not os.path.exists(raw_file):
        return {"available": False, "size": 0, "lines": 0}
        
    size = os.path.getsize(raw_file)
    # Get total lines using wc -l
    lines = 0
    try:
        res = subprocess.run(["wc", "-l", raw_file], capture_output=True, text=True)
        lines = int(res.stdout.split()[0])
    except:
        pass
        
    return {
        "available": True,
        "size_bytes": size,
        "lines": lines
    }
