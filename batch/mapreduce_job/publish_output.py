#!/usr/bin/env python3
"""Validate and safely publish one MapReduce staging directory in HDFS."""

import argparse
import datetime
import os
import re
import shlex
import signal
import subprocess
import sys


def _log(level, job_id, task_id, stage, message):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print("{} {} component=mapreduce-publisher job_id={} task_id={} stage={} {}".format(
        stamp, level, job_id, task_id, stage, message
    ), flush=True)


def _format_command(command):
    """Quote a command using APIs available in the Hadoop image's Python 3.5."""
    return " ".join(shlex.quote(str(part)) for part in command)


def _call(args, runner):
    command = ["hdfs", "dfs"] + list(args)
    result = runner(command)
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    return result.returncode, stdout, stderr, command


def _run_subprocess(command):
    """Run a command with output capture supported by Python 3.5 in Hadoop."""
    return subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, check=False,
    )


def _run(args, runner, job_id, task_id, stage, *, quiet_stdout=False):
    rc, stdout, stderr, command = _call(args, runner)
    if rc:
        _log("ERROR", job_id, task_id, stage,
             "operation={} exit_code={} stderr={!r} stdout={!r}".format(
                 _format_command(command), rc, stderr[-2000:], stdout[-2000:]
             ))
        return False, stdout, stderr
    detail = "detail=validated" if quiet_stdout else "detail={!r}".format(stdout.strip()[-1000:] or "none")
    _log("INFO", job_id, task_id, stage,
         "operation={} result=ok {}".format(_format_command(command), detail))
    return True, stdout, stderr


def _confirmed_exists(path, runner, job_id, task_id, stage):
    rc, stdout, stderr, command = _call(["-test", "-e", path], runner)
    if rc == 0:
        return True
    if rc == 1 and not stdout.strip() and not stderr.strip():
        return False
    _log("ERROR", job_id, task_id, stage,
         "operation={} exit_code={} existence=unknown stderr={!r}; refusing unsafe publication".format(
             _format_command(command), rc, stderr[-1000:]
         ))
    raise RuntimeError("HDFS existence check failed for {} (exit {})".format(path, rc))


def _validate_readable(path, runner, job_id, task_id, stage):
    ok, listing, _ = _run(["-ls", path], runner, job_id, task_id, stage + ".list")
    if not ok:
        return False
    parts = []
    for line in listing.splitlines():
        fields = line.split()
        if fields and re.search(r"/part-[^/]+$", fields[-1]):
            parts.append(fields[-1])
    if not parts:
        _log("ERROR", job_id, task_id, stage,
             "path={} result=no_part_files action=job_output_not_published".format(path))
        return False
    for part in parts:
        # HDFS validates read permission without copying result records through
        # the API process or into any log file.
        ok, _, _ = _run(["-test", "-r", part], runner, job_id, task_id, stage + ".read", quiet_stdout=True)
        if not ok:
            return False
    return True


def publish_staged_output(task_id, final_path, staging_path, job_id, runner=None):
    """Publish validated staging output while retaining/restoring the prior result."""
    runner = runner or _run_subprocess
    safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", job_id)
    backup_path = "{}.backup.{}".format(final_path, safe_id)
    failed_path = "{}.failed.{}.{}.{}".format(
        final_path, safe_id,
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
        os.getpid(),
    )
    lock_path = "{}.__publish_lock".format(final_path)
    lock_acquired = False
    backup_exists = False
    committed = False
    try:
        if not _validate_readable(staging_path, runner, job_id, task_id, "output.validate_staging"):
            return 1
        ok, _, _ = _run(["-mkdir", lock_path], runner, job_id, task_id, "output.publish_lock")
        if not ok:
            _log("ERROR", job_id, task_id, "output.publish_lock",
                 "lock={} result=unavailable_or_held action=leave_existing_output_untouched".format(lock_path))
            return 1
        lock_acquired = True

        if _confirmed_exists(final_path, runner, job_id, task_id, "output.existing_check"):
            if _confirmed_exists(backup_path, runner, job_id, task_id, "output.backup_check"):
                _log("ERROR", job_id, task_id, "output.backup",
                     "path={} result=already_exists action=refuse_to_overwrite_backup".format(backup_path))
                return 1
            ok, _, _ = _run(["-mv", final_path, backup_path], runner, job_id, task_id, "output.backup")
            if not ok:
                # A client-side error can arrive after HDFS completed the rename.
                # Reconcile paths before returning so the old result remains live.
                try:
                    backup_exists = _confirmed_exists(
                        backup_path, runner, job_id, task_id, "output.backup_reconcile"
                    )
                    final_exists = _confirmed_exists(
                        final_path, runner, job_id, task_id, "output.backup_reconcile"
                    )
                    if backup_exists and not final_exists:
                        restored, _, _ = _run(
                            ["-mv", backup_path, final_path], runner, job_id, task_id,
                            "output.backup_reconcile_restore",
                        )
                        if not restored:
                            _log("CRITICAL", job_id, task_id, "output.backup_reconcile_restore",
                                 "restore_failed backup={} action=manual_restore_required".format(backup_path))
                except Exception as reconcile_error:
                    _log("CRITICAL", job_id, task_id, "output.backup_reconcile",
                         "unable_to_confirm_old_result error={!r} backup={} action=inspect_hdfs_before_retry".format(
                             reconcile_error, backup_path
                         ))
                return 1
            backup_exists = True

        ok, _, _ = _run(["-mv", staging_path, final_path], runner, job_id, task_id, "output.publish")
        if not ok:
            raise RuntimeError("failed to move validated staging output into final path")
        if not _validate_readable(final_path, runner, job_id, task_id, "output.validate_published"):
            raise RuntimeError("published output failed post-move readability validation")
        committed = True
        _log("INFO", job_id, task_id, "output.publish", "result=published path={}".format(final_path))
        if backup_exists:
            ok, _, _ = _run(["-rm", "-r", backup_path], runner, job_id, task_id, "output.backup_cleanup")
            if not ok:
                _log("WARNING", job_id, task_id, "output.backup_cleanup",
                     "backup={} preserved_for_manual_cleanup=true".format(backup_path))
        return 0
    except BaseException as exc:
        _log("ERROR", job_id, task_id, "output.publish",
             "exception={!r} final={} staging={} backup={}".format(exc, final_path, staging_path, backup_path))
        if not committed:
            try:
                backup_exists = backup_exists or _confirmed_exists(
                    backup_path, runner, job_id, task_id, "output.rollback_backup_check"
                )
                if backup_exists:
                    if _confirmed_exists(final_path, runner, job_id, task_id, "output.rollback_check"):
                        ok, _, _ = _run(["-mv", final_path, failed_path], runner, job_id, task_id, "output.preserve_failed_candidate")
                        if not ok:
                            raise RuntimeError("could not preserve failed candidate output")
                    ok, _, _ = _run(["-mv", backup_path, final_path], runner, job_id, task_id, "output.rollback")
                    if ok:
                        backup_exists = False
                        _log("WARNING", job_id, task_id, "output.rollback",
                             "result=restored_previous_output path={}".format(final_path))
                    else:
                        _log("CRITICAL", job_id, task_id, "output.rollback",
                             "restore_failed backup={} action=manual_restore_required".format(backup_path))
            except Exception as rollback_error:
                _log("CRITICAL", job_id, task_id, "output.rollback",
                     "restore_failed error={!r} backup={} action=manual_restore_required".format(rollback_error, backup_path))
        return 1
    finally:
        # Remove only this invocation's staging and lock paths. Keep a backup
        # whenever rollback or backup cleanup did not complete.
        try:
            if _confirmed_exists(staging_path, runner, job_id, task_id, "output.staging_cleanup"):
                _run(["-rm", "-r", staging_path], runner, job_id, task_id, "output.staging_cleanup")
        except Exception as exc:
            _log("WARNING", job_id, task_id, "output.staging_cleanup", "cleanup_failed error={!r}".format(exc))
        if lock_acquired:
            _run(["-rmdir", lock_path], runner, job_id, task_id, "output.publish_unlock")


def publish_after_driver(driver_exit_code, task_id, final_path, staging_path, job_id, runner=None):
    """Keep result publication unreachable unless the Hadoop client succeeded."""
    if driver_exit_code:
        _log("ERROR", job_id, task_id, "output.publish",
             "result=skipped reason=mapreduce_driver_failed exit_code={}".format(driver_exit_code))
        return driver_exit_code
    return publish_staged_output(task_id, final_path, staging_path, job_id, runner)


def main(argv=None):
    def handle_termination(signum, _frame):
        raise KeyboardInterrupt("received signal {} during output publication".format(signum))

    signal.signal(signal.SIGTERM, handle_termination)
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--final", required=True)
    parser.add_argument("--staging", required=True)
    parser.add_argument("--job-id", default=os.environ.get("BDA_JOB_ID", "local"))
    parser.add_argument("--driver-exit-code", type=int, default=0)
    args = parser.parse_args(argv)
    return publish_after_driver(
        args.driver_exit_code, args.task, args.final, args.staging, args.job_id,
    )


if __name__ == "__main__":
    sys.exit(main())
