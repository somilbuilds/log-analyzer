"""Small, deterministic helpers for interpreting YARN MapReduce state."""

import re


def parse_yarn_application_status(output):
    """Parse the stable key/value fields printed by ``yarn application -status``."""
    fields = {}
    for line in (output or "").splitlines():
        match = re.match(r"\s*([A-Za-z][A-Za-z -]*?)\s*:\s*(.*?)\s*$", line)
        if match:
            fields[match.group(1).strip().lower().replace(" ", "_")] = match.group(2).strip()
    state = fields.get("state", "").upper()
    final_state = fields.get("final-state", "").upper()
    normalized = {
        "NEW": "submitted",
        "NEW_SAVING": "submitted",
        "SUBMITTED": "submitted",
        "ACCEPTED": "accepted",
        "RUNNING": "running",
    }.get(state)
    if state == "FINISHED":
        normalized = {
            "SUCCEEDED": "succeeded",
            "FAILED": "failed",
            "KILLED": "killed",
        }.get(final_state, "unknown")
    elif state == "FAILED":
        normalized = "failed"
    elif state == "KILLED":
        normalized = "killed"
    return {
        "state": state or "UNKNOWN",
        "final_state": final_state or "UNDEFINED",
        "status": normalized or "unknown",
        "diagnostics": fields.get("diagnostics", ""),
        "tracking_url": fields.get("tracking-url", ""),
        "start_time": fields.get("start-time", ""),
        "finish_time": fields.get("finish-time", ""),
        "application_id": fields.get("application_id", ""),
        "fields": fields,
    }


def combine_mapreduce_status(yarn, client_running, client_exit_code=None):
    """Keep YARN authoritative while reporting local publication completion separately."""
    yarn_status = (yarn or {}).get("status", "unknown")
    publication = "pending" if client_running else (
        "succeeded" if client_exit_code == 0 else
        "failed" if client_exit_code is not None else "unknown"
    )

    query_unavailable = bool((yarn or {}).get("application_id")) and (yarn or {}).get("available") is False
    if query_unavailable:
        status = "unknown"
    elif yarn_status in ("failed", "killed"):
        status = yarn_status
    elif publication == "failed":
        # The shell exits zero only after validating and publishing the output.
        status = "failed"
    elif client_running:
        if yarn_status == "succeeded":
            status = "publishing"
        else:
            status = yarn_status if yarn_status in ("accepted", "running", "submitted") else "submission"
    elif yarn_status == "succeeded" and publication == "succeeded":
        status = "succeeded"
    else:
        # In particular, a zero client exit alone is not a successful YARN job.
        status = "unknown"

    return {
        "status": status,
        "yarn_status": yarn_status,
        "publication_status": publication,
        "running": client_running or status in ("submission", "submitted", "accepted", "running", "publishing"),
        "success": status == "succeeded",
        "code": client_exit_code,
    }
