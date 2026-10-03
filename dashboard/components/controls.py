"""Controls component — time range, filters, demo buttons."""

import os
import sys
import time
import shutil
import subprocess
import streamlit as st


def render():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_replayer_script = os.path.join(base_dir, "ingestion", "log_replayer.py")
    streaming_job_script = os.path.join(base_dir, "streaming", "streaming_job.py")
    raw_access_log = os.path.join(base_dir, "data", "raw", "access_log")
    tmp_stream_dir = "/tmp/log_stream_demo"
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27018/log_analytics")

    if "replayer_proc" not in st.session_state:
        st.session_state.replayer_proc = None
    if "stream_proc" not in st.session_state:
        st.session_state.stream_proc = None
    if "time_range" not in st.session_state:
        st.session_state.time_range = "All"
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = True

    c1, c2, c3, c4, c5, c6 = st.columns([1.2, 1.2, 1.5, 1.5, 1.5, 5.1])

    with c1:
        if st.button("▶ Start Demo", use_container_width=True):
            if os.path.exists(tmp_stream_dir):
                shutil.rmtree(tmp_stream_dir, ignore_errors=True)
            os.makedirs(tmp_stream_dir, exist_ok=True)

            p1 = subprocess.Popen([
                sys.executable, log_replayer_script,
                "--input", raw_access_log,
                "--output-dir", tmp_stream_dir,
                "--speed", "20",
                "--inject-anomaly", "--anomaly-type", "ddos",
            ])
            p2 = subprocess.Popen([
                sys.executable, streaming_job_script,
                "--input-dir", tmp_stream_dir,
                "--mongo-uri", mongo_uri,
                "--mode", "local",
            ])
            st.session_state.replayer_proc = p1
            st.session_state.stream_proc = p2
            time.sleep(2)
            st.rerun()

    with c2:
        if st.button("■ Stop Demo", use_container_width=True):
            for key in ("replayer_proc", "stream_proc"):
                proc = st.session_state.get(key)
                if proc:
                    proc.terminate()
                    st.session_state[key] = None
            time.sleep(1)
            st.rerun()

    with c3:
        st.session_state.time_range = st.selectbox(
            "TIME RANGE",
            ["1m", "5m", "15m", "1h", "All"],
            index=4,
            label_visibility="collapsed",
        )

    with c4:
        st.session_state.auto_refresh = st.checkbox("Auto-refresh", value=True)

    with c5:
        status_filter = st.selectbox(
            "STATUS",
            ["All", "2xx", "3xx", "4xx", "5xx"],
            index=0,
            label_visibility="collapsed",
        )
        st.session_state.status_filter = status_filter

    st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)


def apply_time_filter(df, time_range: str):
    """Filter dataframe to the selected time range by window count."""
    if df.empty or time_range == "All":
        return df
    # Map time ranges to approximate window counts
    # Each window ~= 2-10 seconds depending on pipeline speed
    window_map = {"1m": 12, "5m": 60, "15m": 180, "1h": 720}
    n = window_map.get(time_range, len(df))
    return df.tail(n).reset_index(drop=True)
