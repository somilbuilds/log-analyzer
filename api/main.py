import os
import sys
import subprocess
import shutil
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.queries import (
    get_pipeline_status, fetch_window_aggregates,
    fetch_bloom_stats, fetch_dgim_stats, fetch_fm_stats,
    fetch_recent_events, mongo_ok,
)

app = FastAPI(title="Log Analyzer API", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

demo_procs = {"replayer": None, "stream": None}

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


import signal

def _kill_proc(proc):
    if not proc:
        return
    try:
        if proc.poll() is None:
            # We used start_new_session=True, so we must kill the process group
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

    if not resume:
        if os.path.exists(tmp_stream_dir):
            shutil.rmtree(tmp_stream_dir, ignore_errors=True)
    os.makedirs(tmp_stream_dir, exist_ok=True)

    for key in list(demo_procs):
        _kill_proc(demo_procs[key])
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
    if getattr(locals().get("resume"), "real", resume):
        replayer_args.append("--resume")

    demo_procs["replayer"] = subprocess.Popen(
        replayer_args,
        stdout=replayer_log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )
    demo_procs["stream"] = subprocess.Popen(
        [
            sys.executable, streaming_job_script,
            "--input-dir", tmp_stream_dir,
            "--mongo-uri", mongo_uri,
            "--mode", "local",
        ],
        stdout=stream_log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )

    return {
        "status": "started",
        "replayer_pid": demo_procs["replayer"].pid,
        "stream_pid": demo_procs["stream"].pid,
    }


@app.post("/api/demo/stop")
def stop_demo():
    for key in list(demo_procs):
        _kill_proc(demo_procs[key])
        demo_procs[key] = None
    return {"status": "stopped"}


@app.on_event("shutdown")
def _shutdown():
    stop_demo()
