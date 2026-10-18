#!/usr/bin/env python3
"""Add bounded, per-attempt diagnostics around a Hadoop Streaming command."""

import logging
import os
import runpy
import sys


class CountingInput:
    def __init__(self, stream):
        self.stream = stream
        self.lines = 0

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.stream)
        self.lines += 1
        return line

    def __getattr__(self, name):
        return getattr(self.stream, name)


class CountingOutput:
    def __init__(self, stream):
        self.stream = stream
        self.lines = 0

    def write(self, value):
        self.lines += value.count("\n")
        return self.stream.write(value)

    def __getattr__(self, name):
        return getattr(self.stream, name)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 2 or argv[0] not in ("mapper", "reducer"):
        print("usage: streaming_runner.py mapper|reducer SCRIPT", file=sys.stderr)
        return 2

    stage, script = argv
    job_id = os.environ.get("BDA_JOB_ID", "unknown")
    task_id = os.environ.get("BDA_TASK_ID", "unknown")
    attempt_id = os.environ.get(
        "mapreduce_task_attempt_id",
        os.environ.get("mapred_task_id", "unknown"),
    )
    logger = logging.getLogger("mapreduce.worker")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s component=mapreduce-%(stage)s "
        "job_id=%(job_id)s task_id=%(task_id)s attempt_id=%(attempt_id)s %(message)s",
        "%Y-%m-%dT%H:%M:%S%z",
    ))
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False

    source = CountingInput(sys.stdin)
    destination = CountingOutput(sys.stdout)
    logger.info(
        "event=worker.started script=%s",
        script,
        extra={"stage": stage, "job_id": job_id, "task_id": task_id, "attempt_id": attempt_id},
    )
    sys.stdin = source
    sys.stdout = destination
    sys.argv = [script]
    try:
        runpy.run_path(script, run_name="__main__")
    except BaseException:
        logger.exception(
            "event=worker.failed input_lines=%s output_lines=%s",
            source.lines, destination.lines,
            extra={"stage": stage, "job_id": job_id, "task_id": task_id, "attempt_id": attempt_id},
        )
        return 1
    finally:
        sys.stdin = source.stream
        sys.stdout = destination.stream

    logger.info(
        "event=worker.completed input_lines=%s output_lines=%s",
        source.lines, destination.lines,
        extra={"stage": stage, "job_id": job_id, "task_id": task_id, "attempt_id": attempt_id},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
