"""
Data access layer — MongoDB queries for the API.
No Streamlit dependency. Pure pymongo + pandas.
"""

import os
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/log_analytics")

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return _client


def mongo_ok() -> bool:
    try:
        _get_client().admin.command("ping")
        return True
    except Exception:
        return False


def get_db():
    return _get_client().get_default_database()


def fetch_collection(name: str, limit: int = 500) -> pd.DataFrame:
    """Fetch recent documents sorted by window_id descending."""
    try:
        docs = list(
            get_db()[name]
            .find({}, {"_id": 0})
            .sort("window_id", -1)
            .limit(limit)
        )
        if docs:
            df = pd.DataFrame(docs)
            if "window_id" in df.columns:
                return df.sort_values("window_id").reset_index(drop=True)
            return df.reset_index(drop=True)
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_window_aggregates(limit: int = 500) -> pd.DataFrame:
    return fetch_collection("window_aggregates", limit)


def fetch_bloom_stats(limit: int = 500) -> pd.DataFrame:
    return fetch_collection("bloom_stats", limit)


def fetch_dgim_stats(limit: int = 500) -> pd.DataFrame:
    return fetch_collection("dgim_stats", limit)


def fetch_fm_stats(limit: int = 500) -> pd.DataFrame:
    return fetch_collection("fm_stats", limit)


def fetch_recent_events(limit: int = 80) -> list:
    try:
        docs = list(
            get_db()["recent_events"]
            .find({}, {"_id": 0})
            .sort([("window_id", -1), ("seq", -1)])
            .limit(limit)
        )
        return list(reversed(docs))
    except Exception:
        return []


def get_pipeline_status() -> dict:
    """Check if pipeline is actively producing data."""
    status = {
        "running": False,
        "last_update": None,
        "window_count": 0,
        "mongo": mongo_ok(),
    }
    if not status["mongo"]:
        return status
    try:
        db = get_db()
        latest = db.window_aggregates.find_one(
            {}, {"_id": 0, "window_time": 1, "window_id": 1},
            sort=[("window_id", -1)]
        )
        if latest:
            status["window_count"] = db.window_aggregates.count_documents({})
            status["last_update"] = latest.get("window_time", "")
            try:
                last_dt = datetime.fromisoformat(status["last_update"])
                status["running"] = (datetime.utcnow() - last_dt).total_seconds() < 30
            except (ValueError, TypeError):
                pass
    except Exception:
        pass
    return status
