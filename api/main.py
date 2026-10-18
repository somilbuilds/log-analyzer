"""
ClarkNet Big Data Analytics Laboratory — FastAPI Backend
=========================================================
Provides APIs for: health, dataset, HDFS diagnostics, MapReduce job catalog
(25 tasks), streaming session control, WebSocket live events, job history,
forecasting, and data export.
"""

import asyncio
import json
import logging
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from collections import deque
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, Request
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
    get_yarn_application_status, TASK_CATALOG,
)
from api.mapreduce_logging import (
    attach_job_log_handler, configure_logging, monitor_driver_process,
    should_log_http_request,
)
from api.mapreduce_status import combine_mapreduce_status

app = FastAPI(title="ClarkNet Analytics Laboratory", docs_url="/docs")
logger = logging.getLogger("clarknet")
mapreduce_logger = logging.getLogger("clarknet.mapreduce")


@app.middleware("http")
async def log_http_request(request: Request, call_next):
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "event=http.request.exception method=%s path=%s",
            request.method, request.url.path,
            extra={"component": "api.http"},
        )
        raise
    if should_log_http_request(response.status_code) and response.status_code >= 500:
        logger.error(
            "event=http.request.failed method=%s path=%s status=%s duration_ms=%.1f",
            request.method, request.url.path, response.status_code,
            (time.perf_counter() - started) * 1000,
            extra={"component": "api.http"},
        )
    elif should_log_http_request(response.status_code):
        logger.warning(
            "event=http.request.rejected method=%s path=%s status=%s duration_ms=%.1f",
            request.method, request.url.path, response.status_code,
            (time.perf_counter() - started) * 1000,
            extra={"component": "api.http"},
        )
    return response


@app.post("/api/debug/click")
async def log_click(request: Request):
    click = await request.json()
    # Click metadata is intentionally limited to UI labels and element identifiers.
    logger.info(
        "ui.click route=%s tag=%s label=%r id=%r class=%r x=%s y=%s",
        str(click.get("route", ""))[:200],
        str(click.get("tag", ""))[:32],
        str(click.get("label", ""))[:160],
        str(click.get("id", ""))[:100],
        str(click.get("className", ""))[:160],
        click.get("x"),
        click.get("y"),
    )
    return {"status": "logged"}


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
mapreduce_run = {
    "job_id": "-", "correlation_id": "-", "task_id": "-", "application_id": None,
    "client_exit_code": None, "publication_status": "unknown", "last_yarn_status": None,
    "map_progress": 0, "reduce_progress": 0, "phase": "idle",
}
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
configure_logging(log_dir)

# WebSocket subscribers
ws_subscribers: set = set()


# ── Helpers ──────────────────────────────────────────────────────

def _to_records(df):
    if df.empty:
        return []
    return df.where(df.notna(), None).to_dict(orient="records")


def _kill_proc(proc, name="process", job_id="-", task_id="-"):
    if not proc:
        return
    process_logger = mapreduce_logger if name == "mapreduce" else logger
    component = "mapreduce-api" if name == "mapreduce" else "api.process"
    try:
        if proc.poll() is None:
            process_logger.warning(
                "event=process.termination_requested name=%s pid=%s",
                name, proc.pid,
                extra={"component": component, "job_id": job_id, "task_id": task_id},
            )
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGTERM)
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                process_logger.error(
                    "event=process.termination_escalated name=%s pid=%s signal=SIGKILL",
                    name, proc.pid,
                    extra={"component": component, "job_id": job_id, "task_id": task_id},
                )
                os.killpg(pgid, signal.SIGKILL)
                proc.wait(timeout=2)
        process_logger.info(
            "event=process.exit name=%s pid=%s exit_code=%s",
            name, proc.pid, proc.poll(),
            extra={"component": component, "job_id": job_id, "task_id": task_id},
        )
    except Exception:
        process_logger.exception(
            "event=process.termination_error name=%s pid=%s",
            name, getattr(proc, "pid", "unknown"),
            extra={"component": component, "job_id": job_id, "task_id": task_id},
        )


def _collect_yarn_diagnostics(application_id, job_id, task_id):
    """Save YARN status/container output after a failed driver invocation."""
    path = os.path.join(log_dir, "mapreduce-yarn-{}.log".format(job_id))
    commands = [
        ("application status", ["docker", "exec", "namenode", "yarn", "application", "-status", application_id]),
        ("application logs", ["docker", "exec", "namenode", "yarn", "logs", "-applicationId", application_id]),
    ]
    try:
        with open(path, "a", encoding="utf-8") as output:
            for label, command in commands:
                output.write("=== {} ===\n$ {}\n".format(label, " ".join(command)))
                try:
                    result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
                    body = (result.stdout or "") + (result.stderr or "")
                    if len(body) > 5 * 1024 * 1024:
                        body = body[:5 * 1024 * 1024] + "\n[diagnostic output truncated at 5 MiB]\n"
                    output.write(body or "(no output)\n")
                    output.write("\nexit_code={}\n\n".format(result.returncode))
                    if result.returncode:
                        mapreduce_logger.warning(
                            "event=yarn.diagnostics.command_failed command=%s exit_code=%s detail=%r",
                            label, result.returncode, body[-1000:],
                            extra={"component": "yarn-diagnostics", "job_id": job_id, "task_id": task_id},
                        )
                except subprocess.TimeoutExpired as exc:
                    timeout_output = "\n".join(
                        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else (value or "")
                        for value in (exc.stdout, exc.stderr)
                    )
                    output.write("Timed out after 60 seconds: {}\n{}\n\n".format(exc, timeout_output))
                    mapreduce_logger.error(
                        "event=yarn.diagnostics.timeout command=%s timeout_seconds=60 partial_output=%r",
                        label, timeout_output[-2000:],
                        extra={"component": "yarn-diagnostics", "job_id": job_id, "task_id": task_id},
                    )
        mapreduce_logger.info(
            "event=yarn.diagnostics.saved path=%s application_id=%s",
            path, application_id,
            extra={"component": "yarn-diagnostics", "job_id": job_id, "task_id": task_id},
        )
    except Exception:
        mapreduce_logger.exception(
            "event=yarn.diagnostics.write_failed path=%s",
            path,
            extra={"component": "yarn-diagnostics", "job_id": job_id, "task_id": task_id},
        )


def _persist_mapreduce_history(correlation_id, job_id, task_id, updates):
    """Persist state transitions without allowing a history-store error to hide job state."""
    try:
        db = get_db()
        result = db.job_history.update_one(
            {"correlation_id": correlation_id}, {"$set": updates}
        )
        if not getattr(result, "matched_count", 0):
            mapreduce_logger.warning(
                "event=mapreduce.history_update_missing correlation_id=%s fields=%s",
                correlation_id, sorted(updates),
                extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
            )
    except Exception:
        mapreduce_logger.exception(
            "event=mapreduce.history_update_failed correlation_id=%s fields=%s",
            correlation_id, sorted(updates),
            extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
        )
def _watch_mapreduce_process(proc, job_id, task_id):
    correlation_id = mapreduce_run.get("correlation_id", job_id)
    safe_job_id = re.sub(r"[^A-Za-z0-9_.-]", "_", job_id)
    job_log_path = os.path.join(log_dir, "mapreduce-job-{}.log".format(safe_job_id))
    job_handler = None
    try:
        try:
            job_handler = attach_job_log_handler(mapreduce_logger, job_log_path, job_id, task_id)
        except Exception:
            mapreduce_logger.exception(
                "event=mapreduce.job_log.open_failed path=%s",
                job_log_path,
                extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
            )
        def remember_application_id(application_id):
            if mapreduce_run.get("job_id") == job_id:
                mapreduce_run["application_id"] = application_id
                mapreduce_run["phase"] = "running"
            _persist_mapreduce_history(correlation_id, job_id, task_id, {
                "application_id": application_id,
                "yarn_state": "SUBMITTED",
                "submission_time": datetime.utcnow().isoformat(),
            })

        def record_progress(map_pct, reduce_pct):
            if mapreduce_run.get("job_id") == job_id:
                mapreduce_run["map_progress"] = map_pct
                mapreduce_run["reduce_progress"] = reduce_pct

        def record_phase(phase):
            if mapreduce_run.get("job_id") == job_id:
                mapreduce_run["phase"] = phase

        result = monitor_driver_process(
            proc, job_id, task_id, mapreduce_logger,
            on_application_id=remember_application_id,
            on_progress=record_progress,
            on_phase=record_phase,
        )
        if result["return_code"] == 0:
            mapreduce_run["map_progress"] = 100
            mapreduce_run["reduce_progress"] = 100
            mapreduce_run["phase"] = "completed"
        else:
            mapreduce_run["phase"] = "failed"
        mapreduce_run.update({
            "client_exit_code": result["return_code"],
            "publication_status": "succeeded" if result["return_code"] == 0 else "failed",
        })
        final_yarn = get_yarn_application_status(
            result.get("application_id"), job_id, task_id, timeout=20,
        )
        if not final_yarn.get("available") and mapreduce_run.get("last_yarn_status"):
            cached_yarn = dict(mapreduce_run["last_yarn_status"])
            cached_yarn["query_error"] = final_yarn.get("error")
            final_yarn = cached_yarn
        combined = combine_mapreduce_status(
            final_yarn, client_running=False, client_exit_code=result["return_code"],
        )
        mapreduce_run["status"] = combined["status"]
        history_updates = {
            "client_exit_code": result["return_code"],
            "publication_status": combined["publication_status"],
            "status": combined["status"],
            "yarn_status": final_yarn.get("status", "unknown"),
            "yarn_state": final_yarn.get("state", "UNKNOWN"),
            "final_state": final_yarn.get("final_state", "UNDEFINED"),
            "diagnostics": final_yarn.get("diagnostics", "")[-4000:],
            "tracking_url": final_yarn.get("tracking_url", ""),
            "finish_time": final_yarn.get("finish_time"),
            "yarn_finish_time": final_yarn.get("finish_time", ""),
            "yarn_start_time": final_yarn.get("start_time", ""),
            "end_time": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        started_at = mapreduce_run.get("started_at")
        if started_at:
            try:
                history_updates["duration"] = max(
                    0.0,
                    (datetime.fromisoformat(history_updates["end_time"]) - datetime.fromisoformat(started_at)).total_seconds(),
                )
            except (TypeError, ValueError):
                mapreduce_logger.warning(
                    "event=mapreduce.duration.unavailable started_at=%r",
                    started_at,
                    extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
                )
        _persist_mapreduce_history(correlation_id, job_id, task_id, history_updates)
        mapreduce_logger.info(
            "event=mapreduce.final_state status=%s yarn_status=%s publication_status=%s exit_code=%s application_id=%s diagnostics=%r",
            combined["status"], combined["yarn_status"], combined["publication_status"],
            result["return_code"], result.get("application_id"), final_yarn.get("diagnostics", "")[-1000:],
            extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
        )
        if combined["status"] in ("failed", "killed"):
            if result["application_id"]:
                _collect_yarn_diagnostics(result["application_id"], job_id, task_id)
                next_step = "Inspect the saved YARN diagnostics and ResourceManager logs."
            else:
                next_step = "No YARN application ID was emitted; inspect the driver submission errors and ResourceManager logs."
            mapreduce_logger.error(
                "event=mapreduce.failure.next_step detail=%s",
                next_step,
                extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
            )
    except Exception:
        mapreduce_logger.exception(
            "event=mapreduce.monitor_failed",
            extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
        )
    finally:
        if job_handler is not None:
            mapreduce_logger.removeHandler(job_handler)
            job_handler.close()


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
    result = get_mapreduce_results(task)
    mapreduce_logger.debug(
        "event=mapreduce.results task=%s available=%s rows=%s",
        task, result.get("available"), len(result.get("rows", [])),
        extra={"component": "mapreduce-api", "task_id": task},
    )
    if not result.get("available"):
        mapreduce_logger.warning(
            "event=mapreduce.results.unavailable task=%s detail=%s",
            task, result.get("error", "no result available"),
            extra={"component": "mapreduce-api", "task_id": task},
        )
    return result


@app.get("/api/mapreduce/status")
def mapreduce_status():
    proc = demo_procs.get("mapreduce")
    job_id = mapreduce_run.get("job_id", "-")
    task_id = mapreduce_run.get("task_id", mapreduce_task["value"])
    if proc is None and job_id == "-":
        return {
            "running": False, "status": "idle", "task": mapreduce_task["value"],
            "map_progress": 0, "reduce_progress": 0, "phase": "idle",
        }
    rc = proc.poll() if proc is not None else mapreduce_run.get("client_exit_code")
    client_running = proc is not None and rc is None
    application_id = mapreduce_run.get("application_id")
    # YARN's CLI can take several seconds while the ResourceManager is busy.
    # A 3-second cutoff turned a healthy, completed application into a transient
    # UI warning; retain a bounded query while allowing the status to be recorded.
    yarn_query = get_yarn_application_status(application_id, job_id, task_id, timeout=15) if application_id else {
        "available": False, "status": "unknown", "state": "UNKNOWN",
        "final_state": "UNDEFINED", "diagnostics": "", "tracking_url": "",
    }
    status_stale = False
    if yarn_query.get("available"):
        mapreduce_run["last_yarn_status"] = dict(yarn_query)
        yarn = yarn_query
        state_key = (yarn.get("status"), yarn.get("state"), yarn.get("final_state"))
        if mapreduce_run.get("last_persisted_yarn") != state_key:
            mapreduce_run["last_persisted_yarn"] = state_key
            _persist_mapreduce_history(mapreduce_run.get("correlation_id", job_id), job_id, task_id, {
                "status": combine_mapreduce_status(yarn, client_running, rc)["status"],
                "yarn_status": yarn.get("status"),
                "yarn_state": yarn.get("state"),
                "final_state": yarn.get("final_state"),
                "diagnostics": yarn.get("diagnostics", "")[-4000:],
                "tracking_url": yarn.get("tracking_url", ""),
                "yarn_start_time": yarn.get("start_time", ""),
                "yarn_finish_time": yarn.get("finish_time", ""),
                "updated_at": datetime.utcnow().isoformat(),
            })
    elif mapreduce_run.get("last_yarn_status"):
        yarn = dict(mapreduce_run["last_yarn_status"])
        status_stale = True
        yarn["query_error"] = yarn_query.get("error")
    else:
        yarn = yarn_query
    combined = combine_mapreduce_status(yarn, client_running, rc)
    mapreduce_run["status"] = combined["status"]
    return {
        **combined,
        "task": task_id,
        "job_id": job_id,
        "application_id": application_id,
        "map_progress": mapreduce_run.get("map_progress", 0),
        "reduce_progress": mapreduce_run.get("reduce_progress", 0),
        "phase": mapreduce_run.get("phase", "idle"),
        "yarn_state": yarn.get("state", "UNKNOWN"),
        "final_state": yarn.get("final_state", "UNDEFINED"),
        "diagnostics": yarn.get("diagnostics", ""),
        "tracking_url": yarn.get("tracking_url", ""),
        "start_time": yarn.get("start_time", ""),
        "finish_time": yarn.get("finish_time", ""),
        "status_query_error": yarn_query.get("error"),
        "status_stale": status_stale,
    }


@app.post("/api/mapreduce/run")
def start_mapreduce(task: str = "status"):
    correlation_id = uuid.uuid4().hex
    started_at = datetime.utcnow().isoformat()
    mapreduce_logger.info(
        "event=mapreduce.request_received",
        extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
    )
    # Validate task exists in catalog
    valid_tasks = {t["id"] for t in TASK_CATALOG}
    if task not in valid_tasks:
        mapreduce_logger.warning(
            "event=mapreduce.request_rejected reason=unknown_task",
            extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
        )
        return {"status": "error", "detail": f"Unknown task '{task}'. Check /api/mapreduce/catalog."}

    proc = demo_procs.get("mapreduce")
    if proc and proc.poll() is None:
        mapreduce_logger.warning(
            "event=mapreduce.request_rejected reason=job_already_running pid=%s",
            proc.pid,
            extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
        )
        return {"status": "error", "detail": "MapReduce job already running"}

    try:
        # Ensure dataset is available before running
        dataset_status = ensure_hdfs_dataset(job_id=correlation_id, task_id=task)
        if not dataset_status.get("available"):
            mapreduce_logger.warning(
                "event=hdfs.input.validation_unavailable detail=%r",
                dataset_status.get("message"),
                extra={"component": "hdfs", "job_id": correlation_id, "task_id": task},
            )
            return {
                "status": "error",
                "detail": "HDFS input validation failed; job was not submitted: {}".format(
                    dataset_status.get("message", "unknown HDFS error")
                ),
                "error_kind": dataset_status.get("error_kind"),
            }

        # ── YARN pre-flight ────────────────────────────────────────────────
        # Check that the ResourceManager HTTP API is reachable from inside
        # the namenode container *before* submitting the Hadoop job.
        # Without this, a crashed RM causes the job to hang for 20+ minutes
        # of exponential-backoff YARN retries before returning an error.
        yarn_check = subprocess.run(
            [
                "docker", "exec", "namenode", "bash", "-c",
                "curl -sf --max-time 5 http://resourcemanager:8088/ws/v1/cluster/info > /dev/null 2>&1 && echo ok || echo fail",
            ],
            capture_output=True, text=True, timeout=12, check=False,
        )
        if yarn_check.stdout.strip() != "ok":
            mapreduce_logger.error(
                "event=mapreduce.preflight.yarn_unavailable task=%s detail=%r",
                task, yarn_check.stdout.strip() or yarn_check.stderr.strip(),
                extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
            )
            return {
                "status": "error",
                "detail": (
                    "YARN ResourceManager is unreachable (http://resourcemanager:8088). "
                    "The Docker container may be crashed or not yet healthy. "
                    "Run: docker compose up -d   then wait ~30s and retry."
                ),
                "error_kind": "yarn_unavailable",
            }

        # Ensure at least 1 NodeManager is active to avoid getting stuck in ACCEPTED (allocating containers)
        try:
            with urllib.request.urlopen("http://127.0.0.1:8088/ws/v1/cluster/metrics", timeout=3) as resp:
                metrics = json.loads(resp.read().decode("utf-8")).get("clusterMetrics", {})
                if metrics.get("activeNodes", 0) < 1:
                    subprocess.run(["docker", "compose", "up", "-d", "nodemanager"], check=False, timeout=10)
        except Exception:
            pass

        mapreduce_logger.info(
            "event=mapreduce.preflight.yarn_ok",
            extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
        )
        # ── End YARN pre-flight ───────────────────────────────────────────

        batch_dir = os.path.join(base_dir, "batch")

        mapreduce_logger.info(
            "event=mapreduce.submit.prepare operation=docker_cp destination=namenode:/tmp/batch",
            extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
        )
        subprocess.run(
            ["docker", "cp", batch_dir, "namenode:/tmp/batch"],
            check=True, timeout=30, capture_output=True, text=True,
        )

        mapreduce_task["value"] = task

        # Record job start in MongoDB
        try:
            db = get_db()
            job_doc = {
                "type": "mapreduce",
                "task": task,
                "correlation_id": correlation_id,
                "task_name": next((t["name"] for t in TASK_CATALOG if t["id"] == task), task),
                "start_time": started_at,
                "end_time": None,
                "duration": None,
                "input_records": None,
                "output_records": None,
                "error": None,
                "application_id": None,
                "yarn_status": "unknown",
                "publication_status": "pending",
                "status": "submission",
            }
            result = db.job_history.insert_one(job_doc)
            job_id = str(result.inserted_id)
        except Exception:
            job_id = correlation_id
            mapreduce_logger.exception(
                "event=mapreduce.history_write_failed",
                extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task},
            )

        mapreduce_logger.info(
            "event=mapreduce.job.created correlation_id=%s",
            correlation_id,
            extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task},
        )
        safe_job_id = re.sub(r"[^A-Za-z0-9_.-]", "_", job_id)
        job_log_path = os.path.join(log_dir, "mapreduce-job-{}.log".format(safe_job_id))
        try:
            job_handler = attach_job_log_handler(mapreduce_logger, job_log_path, job_id, task)
            try:
                mapreduce_logger.info(
                    "event=mapreduce.job.created correlation_id=%s",
                    correlation_id,
                    extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task},
                )
            finally:
                mapreduce_logger.removeHandler(job_handler)
                job_handler.close()
        except Exception:
            mapreduce_logger.exception(
                "event=mapreduce.job_log.open_failed path=%s",
                job_log_path,
                extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task},
            )
        command = [
            "docker", "exec", "--env", "BDA_JOB_ID={}".format(job_id),
            "--env", "BDA_TASK_ID={}".format(task),
            "namenode", "bash", "/tmp/batch/run_mapreduce.sh", task,
        ]
        demo_procs["mapreduce"] = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=True,
        )
        mapreduce_run.update({
            "job_id": job_id, "correlation_id": correlation_id, "task_id": task,
            "application_id": None, "client_exit_code": None,
            "publication_status": "pending", "status": "submission",
            "started_at": started_at, "last_persisted_yarn": None,
            "last_yarn_status": None,
            "map_progress": 0, "reduce_progress": 0, "phase": "submitting",
        })
        try:
            job_handler = attach_job_log_handler(mapreduce_logger, job_log_path, job_id, task)
            try:
                mapreduce_logger.info(
                    "event=mapreduce.submit.started pid=%s driver_log=%s job_log=%s",
                    demo_procs["mapreduce"].pid, os.path.join(log_dir, "mapreduce.log"), job_log_path,
                    extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task},
                )
            finally:
                mapreduce_logger.removeHandler(job_handler)
                job_handler.close()
        except Exception:
            mapreduce_logger.exception(
                "event=mapreduce.job_log.write_failed path=%s",
                job_log_path,
                extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task},
            )
        watcher = threading.Thread(
            target=_watch_mapreduce_process,
            args=(demo_procs["mapreduce"], job_id, task),
            name="mapreduce-{}".format(job_id),
            daemon=True,
        )
        watcher.start()
        return {"status": "started", "task": task, "job_id": job_id}
    except Exception as e:
        if isinstance(e, subprocess.CalledProcessError):
            mapreduce_logger.error(
                "event=mapreduce.submit.prepare_failed command=%r exit_code=%s stdout=%r stderr=%r",
                e.cmd, e.returncode, e.stdout, e.stderr,
                extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
            )
        mapreduce_logger.exception(
            "event=mapreduce.submit.failed operation=process_start",
            extra={"component": "mapreduce-api", "job_id": correlation_id, "task_id": task},
        )
        _persist_mapreduce_history(correlation_id, locals().get("job_id", correlation_id), task, {
            "status": "failed",
            "publication_status": "failed",
            "error": str(e),
            "client_exit_code": getattr(e, "returncode", None),
            "end_time": datetime.utcnow().isoformat(),
        })
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
    logger.info("process.start name=replayer pid=%s command=%r log=%s", demo_procs["replayer"].pid, replayer_args, os.path.join(log_dir, "replayer.log"))
    logger.info("process.start name=stream pid=%s command=%r log=%s", demo_procs["stream"].pid, stream_args, os.path.join(log_dir, "stream.log"))

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
    proc = demo_procs.get("mapreduce")
    if proc and proc.poll() is None:
        mapreduce_logger.warning(
            "event=mapreduce.shutdown.cancelling pid=%s",
            proc.pid,
            extra={"component": "mapreduce-api", "job_id": mapreduce_run["job_id"], "task_id": mapreduce_run["task_id"]},
        )
    _kill_proc(
        proc, name="mapreduce", job_id=mapreduce_run["job_id"],
        task_id=mapreduce_run["task_id"],
    )
