"""
ClarkNet Big Data Analytics Laboratory — FastAPI Backend
=========================================================
Provides APIs for: health, dataset, HDFS diagnostics, MapReduce job catalog
(25 tasks), streaming session control, WebSocket live events, job history,
forecasting, and data export.
"""

import asyncio
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import time
from collections import deque
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.queries import (
    get_pipeline_status, fetch_window_aggregates,
    fetch_bloom_stats, fetch_dgim_stats, fetch_fm_stats,
    fetch_recent_events, mongo_ok, get_db,
)
from api.hadoop import (
    get_hdfs_status, get_mapreduce_results, get_dataset_info,
    get_hdfs_report, get_hdfs_fsck, ensure_hdfs_dataset,
    TASK_CATALOG,
)

app = FastAPI(title="ClarkNet Analytics Laboratory", docs_url="/docs")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── State ────────────────────────────────────────────────────────
demo_procs = {"replayer": None, "stream": None, "mapreduce": None}
mapreduce_task = {"value": "status"}
stream_state = {
    "status": "stopped",        # stopped | running | paused
    "speed": 40,
    "position": 0,
    "total_records": 0,
    "processed": 0,
    "start_time": None,
    "elapsed": 0,
    "error_count": 0,
    "window_duration": 10,
    "replay_speed_multiplier": 1.0,
}

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_replayer_script = os.path.join(base_dir, "ingestion", "log_replayer.py")
streaming_job_script = os.path.join(base_dir, "streaming", "streaming_job.py")
raw_access_log = os.path.join(base_dir, "data", "raw", "access_log")
tmp_stream_dir = "/tmp/log_stream_demo"
mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27018/log_analytics")
log_dir = os.path.join(base_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

# WebSocket subscribers
ws_subscribers: set = set()


# ── Helpers ──────────────────────────────────────────────────────

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


def _count_lines(filepath):
    """Count lines in a file efficiently."""
    try:
        res = subprocess.run(["wc", "-l", filepath], capture_output=True, text=True)
        return int(res.stdout.split()[0])
    except Exception:
        return 0


def _dataset_date_range():
    """Extract first and last timestamps from the ClarkNet dataset."""
    import re
    ts_pattern = re.compile(r'\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2})')
    first_ts = last_ts = None
    try:
        with open(raw_access_log, 'r', errors='replace') as f:
            for i, line in enumerate(f):
                m = ts_pattern.search(line)
                if m:
                    if first_ts is None:
                        first_ts = m.group(1)
                    last_ts = m.group(1)
                if i > 5 and first_ts:
                    break
        # Get last timestamp from tail
        res = subprocess.run(["tail", "-20", raw_access_log], capture_output=True, text=True)
        for line in reversed(res.stdout.strip().split('\n')):
            m = ts_pattern.search(line)
            if m:
                last_ts = m.group(1)
                break
    except Exception:
        pass
    return first_ts, last_ts


# ── Health ───────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    hdfs_info = get_hdfs_status()
    return {
        "ok": True,
        "mongo": mongo_ok(),
        "hdfs": hdfs_info.get("available", False),
        "dataset": os.path.isfile(raw_access_log),
        "dataset_ready": hdfs_info.get("dataset_ready", True),
        "yarn": hdfs_info.get("available", False),
        "spark": True,
        "time": datetime.utcnow().isoformat(),
    }


# ── Dataset ──────────────────────────────────────────────────────

@app.get("/api/dataset")
def dataset_info():
    info = get_dataset_info()
    first_ts, last_ts = _dataset_date_range()
    info["date_range"] = {"first": first_ts, "last": last_ts}
    info["format"] = "Common Log Format (CLF)"
    info["fields"] = ["host", "ident", "authuser", "timestamp", "request", "status", "bytes"]
    info["source"] = "ClarkNet HTTP traces (ITA, ita.ee.lbl.gov)"
    return info


# ── HDFS ─────────────────────────────────────────────────────────

@app.get("/api/hdfs/status")
def hdfs_status():
    return get_hdfs_status()


@app.get("/api/hdfs/report")
def hdfs_report():
    return get_hdfs_report()


@app.get("/api/hdfs/fsck")
def hdfs_fsck():
    return get_hdfs_fsck()




# ── MapReduce Task Catalog ───────────────────────────────────────

@app.get("/api/mapreduce/catalog")
def mapreduce_catalog():
    """Return the full catalog of 25 analytical tasks."""
    return {"tasks": TASK_CATALOG}


@app.get("/api/mapreduce/results")
def mapreduce_results(task: str = "status"):
    return get_mapreduce_results(task)


@app.get("/api/mapreduce/status")
def mapreduce_status():
    proc = demo_procs.get("mapreduce")
    if proc is None:
        return {"running": False, "status": "idle", "task": mapreduce_task["value"]}
    rc = proc.poll()
    if rc is None:
        return {"running": True, "status": "running", "task": mapreduce_task["value"]}
    status = "completed" if rc == 0 else "failed"
    return {
        "running": False,
        "status": status,
        "code": rc,
        "task": mapreduce_task["value"],
        "success": rc == 0,
    }


@app.post("/api/mapreduce/run")
def start_mapreduce(task: str = "status"):
    # Validate task exists in catalog
    valid_tasks = {t["id"] for t in TASK_CATALOG}
    if task not in valid_tasks:
        return {"status": "error", "detail": f"Unknown task '{task}'. Check /api/mapreduce/catalog."}

    proc = demo_procs.get("mapreduce")
    if proc and proc.poll() is None:
        return {"status": "error", "detail": "MapReduce job already running"}

    try:
        # Ensure dataset is available before running
        ensure_hdfs_dataset()

        batch_dir = os.path.join(base_dir, "batch")
        subprocess.run(["docker", "cp", batch_dir, "namenode:/tmp/batch"], check=True, timeout=30)

        mapreduce_task["value"] = task
        log_file = open(os.path.join(log_dir, "mapreduce.log"), "w")

        # Record job start in MongoDB
        try:
            db = get_db()
            job_doc = {
                "type": "mapreduce",
                "task": task,
                "task_name": next((t["name"] for t in TASK_CATALOG if t["id"] == task), task),
                "status": "running",
                "start_time": datetime.utcnow().isoformat(),
                "end_time": None,
                "duration": None,
                "input_records": None,
                "output_records": None,
                "error": None,
            }
            result = db.job_history.insert_one(job_doc)
            job_id = str(result.inserted_id)
        except Exception:
            job_id = None

        demo_procs["mapreduce"] = subprocess.Popen(
            ["docker", "exec", "namenode", "bash", "/tmp/batch/run_mapreduce.sh", task],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return {"status": "started", "task": task, "job_id": job_id}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── Streaming Metrics ────────────────────────────────────────────

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


@app.get("/api/status")
def status():
    data = get_pipeline_status()
    data["demo"] = {
        "replayer": bool(demo_procs["replayer"] and demo_procs["replayer"].poll() is None),
        "stream": bool(demo_procs["stream"] and demo_procs["stream"].poll() is None),
    }
    data["stream_state"] = dict(stream_state)
    return data


# ── Stream Control ───────────────────────────────────────────────

@app.post("/api/stream/start")
def start_stream(
    speed: int = 40,
    batch_size: int = 80,
    resume: bool = False,
    inject_anomaly: bool = True,
    anomaly_type: str = "ddos",
):
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
        "--speed", str(speed),
        "--batch-size", str(batch_size),
    ]
    if inject_anomaly:
        replayer_args += [
            "--inject-anomaly",
            "--anomaly-type", anomaly_type,
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

    stream_state.update({
        "status": "running",
        "speed": speed,
        "start_time": datetime.utcnow().isoformat(),
        "replay_speed_multiplier": speed / 40.0,
    })

    # Record stream session in job history
    try:
        db = get_db()
        db.job_history.insert_one({
            "type": "stream",
            "task": "streaming_session",
            "task_name": "Spark Streaming Session",
            "status": "running",
            "start_time": datetime.utcnow().isoformat(),
            "speed": speed,
            "batch_size": batch_size,
            "inject_anomaly": inject_anomaly,
            "anomaly_type": anomaly_type if inject_anomaly else None,
        })
    except Exception:
        pass

    return {
        "status": "started",
        "resume": resume,
        "speed": speed,
        "replayer_pid": demo_procs["replayer"].pid,
        "stream_pid": demo_procs["stream"].pid,
    }


@app.post("/api/stream/stop")
def stop_stream():
    for key in ["replayer", "stream"]:
        _kill_proc(demo_procs.get(key))
        demo_procs[key] = None
    stream_state["status"] = "stopped"
    return {"status": "stopped"}


@app.post("/api/stream/pause")
def pause_stream():
    proc = demo_procs.get("replayer")
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGSTOP)
            stream_state["status"] = "paused"
            return {"status": "paused"}
        except Exception as e:
            return {"status": "error", "detail": str(e)}
    return {"status": "error", "detail": "No running stream to pause"}


@app.post("/api/stream/resume")
def resume_stream():
    proc = demo_procs.get("replayer")
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
            stream_state["status"] = "running"
            return {"status": "running"}
        except Exception as e:
            return {"status": "error", "detail": str(e)}
    return {"status": "error", "detail": "No paused stream to resume"}


@app.post("/api/stream/speed")
def set_stream_speed(speed: int = 40):
    """Change replay speed. Requires restart of replayer — returns instruction."""
    stream_state["speed"] = speed
    stream_state["replay_speed_multiplier"] = speed / 40.0
    return {"status": "ok", "speed": speed, "note": "Speed change takes effect on next start/restart"}


# ── Backward compatibility ───────────────────────────────────────

@app.post("/api/demo/start")
def start_demo(resume: bool = False):
    return start_stream(resume=resume)


@app.post("/api/demo/stop")
def stop_demo():
    return stop_stream()


# ── Job History ──────────────────────────────────────────────────

@app.get("/api/jobs")
def job_history(limit: int = 50):
    try:
        db = get_db()
        docs = list(
            db.job_history
            .find({}, {"_id": 0})
            .sort("start_time", -1)
            .limit(limit)
        )
        return {"jobs": docs}
    except Exception:
        return {"jobs": []}


# ── Forecast ─────────────────────────────────────────────────────

@app.get("/api/forecast")
def forecast(metric: str = "total_requests", horizon: int = 1, alpha: float = 0.3):
    """
    Next-window EWMA forecast.
    Uses exponentially weighted moving average on the last N aggregate windows.
    """
    try:
        aggs = _to_records(fetch_window_aggregates(50))
        if not aggs:
            return {"forecast": None, "error": "No data available"}

        values = [a.get(metric, 0) or 0 for a in aggs]
        if not values:
            return {"forecast": None, "error": f"Metric '{metric}' not found"}

        # EWMA
        ewma = values[0]
        for v in values[1:]:
            ewma = alpha * v + (1 - alpha) * ewma

        # Simple forecast: EWMA value for next window
        current = values[-1]
        forecast_val = round(ewma, 2)

        # Compute historical error for confidence
        errors = []
        pred = values[0]
        for v in values[1:]:
            errors.append(abs(v - pred))
            pred = alpha * v + (1 - alpha) * pred
        avg_error = sum(errors) / len(errors) if errors else 0

        return {
            "metric": metric,
            "current_value": current,
            "forecast": forecast_val,
            "horizon": horizon,
            "method": "EWMA",
            "alpha": alpha,
            "mean_absolute_error": round(avg_error, 2),
            "confidence_band": {
                "lower": round(forecast_val - 2 * avg_error, 2),
                "upper": round(forecast_val + 2 * avg_error, 2),
            },
            "window_count": len(values),
        }
    except Exception as e:
        return {"forecast": None, "error": str(e)}


# ── Export ───────────────────────────────────────────────────────

@app.get("/api/export/batch/{task}")
def export_batch(task: str, format: str = "csv"):
    results = get_mapreduce_results(task)
    if not results.get("available"):
        return {"status": "error", "detail": "No results available for this task"}

    rows = results.get("rows", [])
    if format == "json":
        return StreamingResponse(
            iter([json.dumps(rows, indent=2)]),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={task}_results.json"},
        )

    # CSV
    import io
    buf = io.StringIO()
    if rows:
        headers = list(rows[0].keys())
        buf.write(",".join(headers) + "\n")
        for row in rows:
            buf.write(",".join(str(row.get(h, "")) for h in headers) + "\n")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={task}_results.csv"},
    )


@app.get("/api/export/stream")
def export_stream(format: str = "csv", limit: int = 1000):
    aggs = _to_records(fetch_window_aggregates(limit))
    if format == "json":
        return StreamingResponse(
            iter([json.dumps(aggs, indent=2)]),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=stream_aggregates.json"},
        )

    import io
    buf = io.StringIO()
    if aggs:
        # Flatten status_distribution and top_hosts for CSV
        flat_keys = ["window_id", "window_time", "total_requests", "total_bytes", "error_rate"]
        buf.write(",".join(flat_keys) + "\n")
        for a in aggs:
            buf.write(",".join(str(a.get(k, "")) for k in flat_keys) + "\n")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=stream_aggregates.csv"},
    )


# ── WebSocket for live streaming events ──────────────────────────

@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    ws_subscribers.add(websocket)
    last_window = 0
    try:
        while True:
            try:
                events = fetch_recent_events(30)
                aggs = _to_records(fetch_window_aggregates(5))
                bloom = _to_records(fetch_bloom_stats(5))
                dgim = _to_records(fetch_dgim_stats(5))
                fm = _to_records(fetch_fm_stats(5))

                current_window = aggs[-1]["window_id"] if aggs else 0
                if current_window > last_window:
                    last_window = current_window
                    await websocket.send_json({
                        "type": "update",
                        "events": events[-20:],
                        "aggregates": aggs,
                        "bloom": bloom,
                        "dgim": dgim,
                        "fm": fm,
                        "stream_state": dict(stream_state),
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                else:
                    await websocket.send_json({
                        "type": "heartbeat",
                        "stream_state": dict(stream_state),
                        "timestamp": datetime.utcnow().isoformat(),
                    })
            except Exception as e:
                await websocket.send_json({"type": "error", "detail": str(e)})

            await asyncio.sleep(2)
    except WebSocketDisconnect:
        ws_subscribers.discard(websocket)
    except Exception:
        ws_subscribers.discard(websocket)


# ── Lifecycle ────────────────────────────────────────────────────

@app.on_event("shutdown")
def _shutdown():
    stop_stream()
    _kill_proc(demo_procs.get("mapreduce"))
