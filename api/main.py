import os
import sys
import subprocess
import shutil
import signal
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.queries import (
    get_pipeline_status, fetch_window_aggregates,
    fetch_bloom_stats, fetch_dgim_stats, fetch_fm_stats,
    fetch_recent_events, mongo_ok,
)
from api.hadoop import get_hdfs_status, get_mapreduce_results, get_dataset_info, ensure_hdfs_dataset

app = FastAPI(title="Log Analyzer API", docs_url="/docs")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

demo_procs = {"replayer": None, "stream": None, "mapreduce": None}
mapreduce_task = {"value": "status"}

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_replayer_script = os.path.join(base_dir, "ingestion", "log_replayer.py")
streaming_job_script = os.path.join(base_dir, "streaming", "streaming_job.py")
raw_access_log = os.path.join(base_dir, "data", "raw", "access_log")
tmp_stream_dir = "/tmp/log_stream_demo"
mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27018/log_analytics")
log_dir = os.path.join(base_dir, "logs")
os.makedirs(log_dir, exist_ok=True)


def _to_records(df):
    if df.empty:
        return []
    return df.where(df.notna(), None).to_dict(orient="records")


def _kill_proc(proc):
    if not proc:
        return
    try:
        if proc.poll() is None:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGTERM)
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                os.killpg(pgid, signal.SIGKILL)
    except Exception:
        pass


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "mongo": mongo_ok(),
        "dataset": os.path.isfile(raw_access_log),
        "time": datetime.utcnow().isoformat(),
    }


@app.get("/api/status")
def status():
    data = get_pipeline_status()
    data["demo"] = {
        "replayer": bool(demo_procs["replayer"] and demo_procs["replayer"].poll() is None),
        "stream": bool(demo_procs["stream"] and demo_procs["stream"].poll() is None),
    }
    return data


@app.get("/api/dataset")
def dataset_info():
    return get_dataset_info()


@app.get("/api/hdfs/status")
def hdfs_status():
    return get_hdfs_status()


@app.post("/api/hdfs/upload")
def upload_hdfs_dataset():
    return ensure_hdfs_dataset()


@app.get("/api/mapreduce/results")
def mapreduce_results(task: str = "status"):
    return get_mapreduce_results(task)


@app.get("/api/mapreduce/status")
def mapreduce_status():
    proc = demo_procs.get("mapreduce")
    if proc is None:
        return {"running": False, "status": "idle", "task": mapreduce_task["value"]}
    if proc.poll() is None:
        return {"running": True, "status": "running", "task": mapreduce_task["value"]}
    return {
        "running": False,
        "status": "completed" if proc.returncode == 0 else "failed",
        "code": proc.returncode,
        "task": mapreduce_task["value"],
    }


@app.post("/api/mapreduce/run")
def start_mapreduce(task: str = "status"):
    if task not in {"status", "hosts", "endpoints"}:
        return {"status": "error", "detail": "Unknown task. Use status, hosts, or endpoints."}

    proc = demo_procs.get("mapreduce")
    if proc and proc.poll() is None:
        return {"status": "error", "detail": "MapReduce job already running"}

    try:
        upload = ensure_hdfs_dataset()
        if not upload.get("available"):
            return {"status": "error", "detail": upload.get("message", "HDFS dataset upload failed")}

        batch_dir = os.path.join(base_dir, "batch")
        subprocess.run(["docker", "cp", batch_dir, "namenode:/tmp/batch"], check=False)

        mapreduce_task["value"] = task
        log_file = open(os.path.join(log_dir, "mapreduce.log"), "w")
        demo_procs["mapreduce"] = subprocess.Popen(
            ["docker", "exec", "namenode", "bash", "/tmp/batch/run_mapreduce.sh", task],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return {"status": "started", "task": task}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/metrics")
def metrics(limit: int = 120):
    return {
        "aggregates": _to_records(fetch_window_aggregates(limit)),
        "bloom": _to_records(fetch_bloom_stats(limit)),
        "dgim": _to_records(fetch_dgim_stats(limit)),
        "fm": _to_records(fetch_fm_stats(limit)),
    }


@app.get("/api/events")
def events(limit: int = 60):
    return {"events": fetch_recent_events(limit)}


@app.post("/api/demo/start")
def start_demo(resume: bool = False):
    if not os.path.isfile(raw_access_log):
        return {"status": "error", "detail": "ClarkNet access_log missing. Run scripts/download_clarknet.sh"}

    if not resume and os.path.exists(tmp_stream_dir):
        shutil.rmtree(tmp_stream_dir, ignore_errors=True)
    os.makedirs(tmp_stream_dir, exist_ok=True)

    for key in ["replayer", "stream"]:
        _kill_proc(demo_procs.get(key))
        demo_procs[key] = None

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYSPARK_PYTHON"] = env.get("PYSPARK_PYTHON", sys.executable)

    replayer_log = open(os.path.join(log_dir, "replayer.log"), "a" if resume else "w")
    stream_log = open(os.path.join(log_dir, "stream.log"), "a" if resume else "w")

    replayer_args = [
        sys.executable, log_replayer_script,
        "--input", raw_access_log,
        "--output-dir", tmp_stream_dir,
        "--speed", "40",
        "--batch-size", "80",
        "--inject-anomaly", "--anomaly-type", "ddos",
        "--anomaly-start", "4000",
        "--anomaly-duration", "1600",
    ]
    if resume:
        replayer_args.append("--resume")

    stream_args = [
        sys.executable, streaming_job_script,
        "--input-dir", tmp_stream_dir,
        "--mongo-uri", mongo_uri,
        "--mode", "local",
    ]
    if resume:
        stream_args.append("--resume")

    demo_procs["replayer"] = subprocess.Popen(
        replayer_args,
        stdout=replayer_log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )
    demo_procs["stream"] = subprocess.Popen(
        stream_args,
        stdout=stream_log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )

    return {
        "status": "started",
        "resume": resume,
        "replayer_pid": demo_procs["replayer"].pid,
        "stream_pid": demo_procs["stream"].pid,
    }


@app.post("/api/demo/stop")
def stop_demo():
    for key in ["replayer", "stream"]:
        _kill_proc(demo_procs.get(key))
        demo_procs[key] = None
    return {"status": "stopped"}


@app.on_event("shutdown")
def _shutdown():
    stop_demo()
    _kill_proc(demo_procs.get("mapreduce"))
