"""Shared structured logging and Hadoop driver output parsing.

This module intentionally uses only the Python standard library so its logging
behavior can be tested without importing the API or requiring Docker/Hadoop.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from typing import Callable, Optional


APP_LOG_FORMAT = (
    "%(asctime)s %(levelname)s component=%(component)s "
    "job_id=%(job_id)s task_id=%(task_id)s %(message)s"
)
APP_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


class ContextDefaults(logging.Filter):
    """Populate fields used by the shared formatter on third-party records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "component"):
            record.component = record.name
        if not hasattr(record, "job_id"):
            record.job_id = "-"
        if not hasattr(record, "task_id"):
            record.task_id = "-"
        return True


class JobContextDefaults(ContextDefaults):
    def __init__(self, job_id: str, task_id: str):
        super().__init__()
        self.job_id = job_id
        self.task_id = task_id

    def filter(self, record: logging.LogRecord) -> bool:
        super().filter(record)
        if record.job_id == "-":
            record.job_id = self.job_id
        if record.task_id == "-":
            record.task_id = self.task_id
        return True


def attach_job_log_handler(logger: logging.Logger, path: str, job_id: str, task_id: str):
    """Attach an append-only, per-job log handler; caller must detach/close it."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(APP_LOG_FORMAT, APP_DATE_FORMAT))
    handler.addFilter(JobContextDefaults(job_id, task_id))
    logger.addHandler(handler)
    return handler


def configure_logging(log_dir: str, level: Optional[str] = None) -> None:
    """Route concise INFO+ output to stdout and detailed MapReduce logs to disk."""
    os.makedirs(log_dir, exist_ok=True)
    configured_level = (level or os.environ.get("LOG_LEVEL", "DEBUG")).upper()
    numeric_level = getattr(logging, configured_level, logging.DEBUG)
    formatter = logging.Formatter(APP_LOG_FORMAT, APP_DATE_FORMAT)
    context_filter = ContextDefaults()

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    console.addFilter(context_filter)
    root.addHandler(console)

    app_logger = logging.getLogger("clarknet")
    app_logger.setLevel(numeric_level)
    app_logger.propagate = True

    mapreduce_logger = logging.getLogger("clarknet.mapreduce")
    for old_handler in mapreduce_logger.handlers:
        old_handler.close()
    mapreduce_logger.handlers.clear()
    mapreduce_logger.setLevel(logging.DEBUG)
    mapreduce_logger.propagate = False
    summary = RotatingFileHandler(
        os.path.join(log_dir, "mapreduce.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    summary.setLevel(logging.INFO)
    summary.setFormatter(formatter)
    summary.addFilter(context_filter)
    mapreduce_logger.addHandler(summary)

    details = RotatingFileHandler(
        os.path.join(log_dir, "mapreduce-debug.log"),
        maxBytes=25 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    details.setLevel(logging.DEBUG)
    details.setFormatter(formatter)
    details.addFilter(context_filter)
    mapreduce_logger.addHandler(details)

    # Uvicorn's access logger emits every successful poll independently of the
    # application's request middleware. Keep its failure records, suppress 2xx.
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers.clear()
    access_logger.setLevel(logging.WARNING)
    access_logger.propagate = True
    for logger_name in ("uvicorn", "uvicorn.error"):
        server_logger = logging.getLogger(logger_name)
        server_logger.handlers.clear()
        server_logger.setLevel(logging.INFO)
        server_logger.propagate = True


def should_log_http_request(status_code: int) -> bool:
    """Only HTTP failures are access events; successful polling stays quiet."""
    return status_code >= 400


_APP_ID_RE = re.compile(r"\b(application_\d+_\d+)\b")
_JOB_ID_RE = re.compile(r"\b(job_\d+_\d+)\b")
_PROGRESS_RE = re.compile(r"\b(map|reduce)\s+(\d+)%", re.IGNORECASE)
_COUNTER_RE = re.compile(
    r"^\s*(Map input records|Map output records|Reduce input records|"
    r"Reduce output records|Launched map tasks|Launched reduce tasks|"
    r"Failed Shuffles)\s*=\s*(\d+)\s*$"
)
_ERROR_RE = re.compile(
    r"(?i)(?:^|\s)(?:ERROR|FATAL)\b|\b(?:Exception|FAILED|BlockMissingException|"
    r"connection refused|timed out|timeout)\b"
)
_WARNING_RE = re.compile(r"(?i)(?:^|\s)(?:WARN|WARNING)\b")
_SHELL_EVENT_RE = re.compile(
    r"^\S+\s+(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+"
    r"component=([^\s]+)\s+job_id=([^\s]+)\s+task_id=([^\s]+)\s+"
    r"stage=([^\s]+)\s*(.*)$"
)


def parse_driver_line(line: str) -> dict:
    """Extract actionable events from one Hadoop Streaming client output line."""
    clean = line.strip()
    shell_event = _SHELL_EVENT_RE.match(clean)
    if shell_event:
        level, component, job_id, task_id, stage, message = shell_event.groups()
        return {
            "message": message,
            "level": getattr(logging, level),
            "component": component,
            "job_id": job_id,
            "task_id": task_id,
            "event": "mapreduce.{}".format(stage),
        }
    result = {"message": clean, "level": logging.DEBUG}
    app_match = _APP_ID_RE.search(clean)
    job_match = _JOB_ID_RE.search(clean)
    if app_match:
        result["application_id"] = app_match.group(1)
        result["level"] = logging.INFO
        result["event"] = "yarn.application.accepted"
    if job_match:
        result["hadoop_job_id"] = job_match.group(1)
        if "event" not in result:
            result["level"] = logging.INFO
            result["event"] = "mapreduce.job"

    progress = {key.lower(): int(value) for key, value in _PROGRESS_RE.findall(clean)}
    if progress:
        result["progress"] = progress
        result["level"] = logging.DEBUG
        result["event"] = "mapreduce.progress"

    if _ERROR_RE.search(clean):
        result["level"] = logging.ERROR
        result["event"] = "hadoop.error"
    elif _WARNING_RE.search(clean):
        result["level"] = logging.WARNING
        result["event"] = "hadoop.warning"
    elif "tracking url" in clean.lower():
        result["level"] = logging.INFO
        result["event"] = "yarn.tracking"
    elif "completed successfully" in clean.lower():
        result["level"] = logging.INFO
        result["event"] = "mapreduce.job.completed"
    return result


def monitor_driver_process(
    process: subprocess.Popen,
    job_id: str,
    task_id: str,
    logger: logging.Logger,
    on_application_id: Optional[Callable[[str], None]] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
    on_phase: Optional[Callable[[str], None]] = None,
) -> dict:
    """Log Hadoop client output, summarize progress, and preserve all lines.

    DEBUG records are kept by the MapReduce file handler; INFO+ events also go
    to the terminal. The returned IDs and exit code can drive post-failure
    YARN diagnostics.
    """
    application_id = None
    hadoop_job_id = None
    last_progress = {"map": 0, "reduce": 0}
    counters = {}
    stream = process.stdout
    if stream is not None:
        for raw_line in stream:
            line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else raw_line
            event = parse_driver_line(line)
            msg_text = event.get("message", "")
            if "[1/4]" in msg_text or "input_directories" in msg_text or "input_validation" in msg_text:
                if on_phase:
                    on_phase("validating_input")
            elif "[2/4]" in msg_text or "output_staging" in msg_text:
                if on_phase:
                    on_phase("staging")
            elif "[3/4]" in msg_text:
                if on_phase:
                    on_phase("submitting")
            elif "[4/4]" in msg_text or "output_publish" in msg_text:
                if on_phase:
                    on_phase("publishing")

            if event.get("application_id"):
                application_id = event["application_id"]
                if on_application_id:
                    on_application_id(application_id)
                if on_phase:
                    on_phase("running")
            if event.get("hadoop_job_id"):
                hadoop_job_id = event["hadoop_job_id"]

            counter_match = _COUNTER_RE.match(event["message"])
            if counter_match:
                counters[counter_match.group(1).lower().replace(" ", "_")] = int(counter_match.group(2))
                logger.debug(
                    "event=mapreduce.counter name=%s value=%s",
                    counter_match.group(1), counter_match.group(2),
                    extra={"component": "hadoop-driver", "job_id": job_id, "task_id": task_id},
                )

            progress = event.get("progress")
            if progress:
                changed = {
                    stage: value
                    for stage, value in progress.items()
                    if value == 100 or value - last_progress[stage] >= 5
                }
                for stage, value in progress.items():
                    last_progress[stage] = max(last_progress[stage], value)
                if on_progress:
                    on_progress(max(0, last_progress["map"]), max(0, last_progress["reduce"]))
                if on_phase:
                    if last_progress["reduce"] > 0:
                        on_phase("reducing")
                    else:
                        on_phase("mapping")
                if changed:
                    logger.info(
                        "event=mapreduce.progress map_pct=%s reduce_pct=%s detail=%r",
                        progress.get("map", last_progress["map"]),
                        progress.get("reduce", last_progress["reduce"]),
                        event["message"],
                        extra={"component": "hadoop-driver", "job_id": job_id, "task_id": task_id},
                    )
                else:
                    logger.debug(
                        "event=mapreduce.progress detail=%r",
                        event["message"],
                        extra={"component": "hadoop-driver", "job_id": job_id, "task_id": task_id},
                    )
                continue

            message = event["message"] or "(empty Hadoop output line)"
            event_name = event.get("event", "hadoop.driver.output")
            logger.log(
                event["level"],
                "event=%s detail=%r",
                event_name,
                message,
                extra={
                    "component": event.get("component", "hadoop-driver"),
                    "job_id": event.get("job_id", job_id),
                    "task_id": event.get("task_id", task_id),
                },
            )
        stream.close()

    return_code = process.wait()
    outcome = "completed" if return_code == 0 else "failed"
    if counters:
        logger.info(
            "event=mapreduce.counters summary=%s",
            " ".join("{}={}".format(key, value) for key, value in counters.items()),
            extra={"component": "hadoop-driver", "job_id": job_id, "task_id": task_id},
        )
    logger.log(
        logging.INFO if return_code == 0 else logging.ERROR,
        "event=mapreduce.%s exit_code=%s application_id=%s hadoop_job_id=%s",
        outcome,
        return_code,
        application_id or "unknown",
        hadoop_job_id or "unknown",
        extra={"component": "mapreduce-api", "job_id": job_id, "task_id": task_id},
    )
    return {
        "return_code": return_code,
        "application_id": application_id,
        "hadoop_job_id": hadoop_job_id,
    }
