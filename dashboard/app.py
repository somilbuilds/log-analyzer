#!/usr/bin/env python3
"""
Streamlit Dashboard — Real-Time Web Log Analytics
===================================================

Displays live analytics from MongoDB, auto-refreshing every 5 seconds:
  - Requests per window (live chart)
  - Rolling error rate
  - Bloom Filter seen/new host counts
  - DGIM approximate vs exact 5xx error count comparison
  - Flajolet-Martin estimated vs exact distinct hosts
  - Top hosts / top paths tables
  - R-generated anomaly detection plots

Usage:
  pip install streamlit pymongo pandas plotly
  streamlit run dashboard/app.py -- --mongo-uri mongodb://localhost:27018/log_analytics

Or with default MongoDB URI:
  streamlit run dashboard/app.py
"""

import os
import sys
import time
import subprocess
import shutil
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pymongo import MongoClient

# ─── Page Config ─────────────────────────────────────────────
st.set_page_config(
    page_title="Web Log Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Custom CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    /* Reset padding to reduce whitespace */
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 0rem !important;
        max-width: 95% !important;
    }
    
    /* Dark Theme System */
    :root {
        --bg-color: #0d1117;
        --card-bg: #161b22;
        --accent-color: #2f81f7;
        --text-primary: #c9d1d9;
        --text-muted: #8b949e;
        --border-color: #30363d;
    }
    
    body, .stApp {
        background-color: var(--bg-color) !important;
        color: var(--text-primary) !important;
    }
    
    /* Metric Cards Styling */
    div[data-testid="metric-container"] {
        background-color: var(--card-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 1.25rem 1rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        text-align: center;
        margin-bottom: 1rem;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }
    div[data-testid="metric-container"] > div {
        color: var(--accent-color) !important;
        font-size: 2.2rem !important;
        font-weight: 700;
        margin: auto;
    }
    div[data-testid="metric-container"] label {
        color: var(--text-muted) !important;
        font-size: 0.95rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
    }
    
    /* Headers & Typography */
    h1, h2, h3, h4 { 
        color: var(--text-primary) !important; 
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; 
    }
    h1 { 
        font-weight: 600; 
        padding-bottom: 0.75rem; 
        border-bottom: 1px solid var(--border-color); 
        margin-bottom: 1rem;
    }
    h3 {
        font-size: 1.1rem;
        font-weight: 500;
        margin-bottom: 0.5rem;
    }
    hr {
        border-color: var(--border-color) !important;
    }
    
    /* Generic panel wrapper for charts/tables to keep visually consistent */
    .stPlotlyChart {
        background-color: var(--card-bg);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 0.5rem;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 3rem;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1rem;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        color: var(--accent-color) !important;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ─── MongoDB Connection ─────────────────────────────────────
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018/log_analytics")

@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI)

client = get_mongo_client()
db = client.get_default_database()

# ─── Header ──────────────────────────────────────────────────
st.title("Real-Time Web Log Analytics")
st.markdown("<p style='color:#8b949e; margin-top:-1rem; margin-bottom:1.5rem;'>Streaming pipeline: Log Replayer → Spark/Local Processor → MongoDB → Dashboard</p>", unsafe_allow_html=True)

tab_live, tab_batch = st.tabs(["Live Stream", "Batch Analysis"])

# ─── Fetch Data from MongoDB ────────────────────────────────
def fetch_collection(collection_name, limit=200):
    """Fetch recent documents from a MongoDB collection."""
    try:
        docs = list(db[collection_name].find(
            {}, {"_id": 0}
        ).sort("window_id", -1).limit(limit))
        if docs:
            return pd.DataFrame(docs)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching {collection_name}: {e}")
        return pd.DataFrame()

# Fetch all data
agg_df = fetch_collection("window_aggregates")
bloom_df = fetch_collection("bloom_stats")
dgim_df = fetch_collection("dgim_stats")
fm_df = fetch_collection("fm_stats")

# Prepare paths for Demo
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_replayer_script = os.path.join(base_dir, "ingestion", "log_replayer.py")
streaming_job_script = os.path.join(base_dir, "streaming", "streaming_job.py")
raw_access_log = os.path.join(base_dir, "data", "raw", "access_log")
tmp_stream_dir = "/tmp/log_stream_demo"

if 'replayer_proc' not in st.session_state:
    st.session_state.replayer_proc = None
if 'stream_proc' not in st.session_state:
    st.session_state.stream_proc = None

with tab_live:
    
    col_demo1, col_demo2, col_demo_gap = st.columns([2, 2, 8])
    with col_demo1:
        if st.button("Start Live Demo"):
            if os.path.exists(tmp_stream_dir):
                shutil.rmtree(tmp_stream_dir, ignore_errors=True)
            os.makedirs(tmp_stream_dir, exist_ok=True)

            p1 = subprocess.Popen([
                sys.executable, log_replayer_script,
                "--input", raw_access_log,
                "--output-dir", tmp_stream_dir,
                "--speed", "20",  
                "--inject-anomaly", "--anomaly-type", "ddos"
            ])
            p2 = subprocess.Popen([
                sys.executable, streaming_job_script,
                "--input-dir", tmp_stream_dir,
                "--mongo-uri", MONGO_URI,
                "--mode", "local"
            ])
            st.session_state.replayer_proc = p1
            st.session_state.stream_proc = p2
            st.success("Background processes started! Simulating live data flow.")
            time.sleep(2)
            st.rerun()

    with col_demo2:
        if st.button("Stop Demo"):
            if st.session_state.replayer_proc:
                st.session_state.replayer_proc.terminate()
                st.session_state.replayer_proc = None
            if st.session_state.stream_proc:
                st.session_state.stream_proc.terminate()
                st.session_state.stream_proc = None
            st.success("Background processes stopped.")
            time.sleep(1)
            st.rerun()

    st.markdown("<hr style='margin-top:0.5rem; margin-bottom:1rem;'/>", unsafe_allow_html=True)

    if agg_df.empty:
        st.warning("No data in MongoDB yet. Click 'Start Live Demo' above to begin.")
    else:
        # Sort by window_id ascending for time-series plots
        agg_df = agg_df.sort_values("window_id").reset_index(drop=True)
        if not bloom_df.empty:
            bloom_df = bloom_df.sort_values("window_id").reset_index(drop=True)
        if not dgim_df.empty:
            dgim_df = dgim_df.sort_values("window_id").reset_index(drop=True)
        if not fm_df.empty:
            fm_df = fm_df.sort_values("window_id").reset_index(drop=True)

        # ─── KPI Metrics Row ────────────────────────────────────────
        col1, col2, col3, col4, col5 = st.columns(5)

        total_requests = agg_df["total_requests"].sum()
        avg_error_rate = agg_df["error_rate"].mean() * 100
        total_windows = len(agg_df)
        latest_rps = agg_df.iloc[-1]["total_requests"] if len(agg_df) > 0 else 0

        with col1:
            st.metric("Total Requests", f"{total_requests:,}")
        with col2:
            st.metric("Windows Processed", f"{total_windows}")
        with col3:
            st.metric("Avg Error Rate", f"{avg_error_rate:.2f}%")
        with col4:
            st.metric("Latest Window Size", f"{latest_rps:,}")
        with col5:
            if not bloom_df.empty:
                total_unique = bloom_df.iloc[-1].get("total_items_in_filter", 0)
                st.metric("Unique Hosts (Bloom)", f"{total_unique:,}")
            else:
                st.metric("Unique Hosts", "N/A")

        st.write("")

        # ─── Inner Tabs ─────────────────────────────────────────────
        sub_traffic, sub_algo, sub_tables = st.tabs(["Traffic Overview", "Stream Algorithms", "Top Hosts & Paths"])

        with sub_traffic:
            col_t1, col_t2 = st.columns(2)
            with col_t1:
                st.subheader("Requests per Window")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=agg_df["window_id"], y=agg_df["total_requests"],
                    mode="lines+markers", name="Requests",
                    line=dict(color="#2f81f7", width=2),
                    marker=dict(size=4),
                ))
                fig.update_layout(
                    height=300, margin=dict(l=10, r=10, t=10, b=10),
                    template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_t2:
                st.subheader("Rolling Error Rate")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=agg_df["window_id"], y=agg_df["error_rate"] * 100,
                    mode="lines", name="Error Rate %",
                    fill="tozeroy",
                    line=dict(color="#f85149", width=2),
                    fillcolor="rgba(248, 81, 73, 0.2)",
                ))
                fig.update_layout(
                    height=300, margin=dict(l=10, r=10, t=10, b=10),
                    template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

        with sub_algo:
            col_a1, col_a2, col_a3 = st.columns(3)
            with col_a1:
                st.subheader("Bloom Filter (New vs Seen)")
                if not bloom_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=bloom_df["window_id"], y=bloom_df["new_hosts"],
                        name="New Hosts", marker_color="#3fb950",
                    ))
                    fig.add_trace(go.Bar(
                        x=bloom_df["window_id"], y=bloom_df["seen_hosts"],
                        name="Seen Hosts", marker_color="#8b949e",
                    ))
                    fig.update_layout(
                        barmode="stack", height=300,
                        margin=dict(l=10, r=10, t=10, b=10),
                        template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    latest = bloom_df.iloc[-1]
                    st.caption(
                        f"Memory: {latest.get('filter_memory_bytes', 0):,} bytes | "
                        f"Est FP rate: {latest.get('estimated_fp_rate', 0):.4%}"
                    )
                else:
                    st.info("No Bloom filter data yet.")

            with col_a2:
                st.subheader("DGIM (Approx vs Exact 5xx)")
                if not dgim_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=dgim_df["window_id"], y=dgim_df["dgim_approximate_5xx"],
                        mode="lines+markers", name="DGIM Approx",
                        line=dict(color="#d29922", width=2, dash="dash"),
                        marker=dict(size=4),
                    ))
                    fig.add_trace(go.Scatter(
                        x=dgim_df["window_id"], y=dgim_df["exact_5xx_total"],
                        mode="lines+markers", name="Exact",
                        line=dict(color="#2f81f7", width=2),
                        marker=dict(size=3),
                    ))
                    fig.update_layout(
                        height=300, margin=dict(l=10, r=10, t=10, b=10),
                        template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    latest = dgim_df.iloc[-1]
                    approx = latest.get("dgim_approximate_5xx", 0)
                    exact = latest.get("exact_5xx_total", 0)
                    error = abs(approx - exact) / max(exact, 1) * 100 if exact > 0 else 0
                    st.caption(f"Error: {error:.1f}% | Buckets: {latest.get('dgim_num_buckets', 0)}")
                else:
                    st.info("No DGIM data yet.")

            with col_a3:
                st.subheader("Flajolet-Martin (Distinct Hosts)")
                if not fm_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=fm_df["window_id"], y=fm_df["fm_estimated_distinct_hosts"],
                        mode="lines+markers", name="FM Estimate",
                        line=dict(color="#bc8cff", width=2, dash="dot"),
                        marker=dict(size=4),
                    ))
                    fig.add_trace(go.Scatter(
                        x=fm_df["window_id"], y=fm_df["exact_distinct_hosts"],
                        mode="lines+markers", name="Exact",
                        line=dict(color="#3fb950", width=2),
                        marker=dict(size=3),
                    ))
                    fig.update_layout(
                        height=300, margin=dict(l=10, r=10, t=10, b=10),
                        template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    latest = fm_df.iloc[-1]
                    st.caption(f"Error: {latest.get('fm_error_pct', 0):.1f}%")
                else:
                    st.info("No Flajolet-Martin data yet.")

        with sub_tables:
            col_s1, col_s2, col_s3 = st.columns(3)
            with col_s1:
                st.subheader("Status Code Distribution")
                if len(agg_df) > 0:
                    latest_status = agg_df.iloc[-1].get("status_distribution", {})
                    if latest_status and isinstance(latest_status, dict):
                        status_data = pd.DataFrame([
                            {"Status Code": k, "Count": v}
                            for k, v in latest_status.items()
                        ]).sort_values("Count", ascending=False)

                        color_map = {}
                        for code in status_data["Status Code"]:
                            if code.startswith("2"): color_map[code] = "#3fb950"
                            elif code.startswith("3"): color_map[code] = "#2f81f7"
                            elif code.startswith("4"): color_map[code] = "#d29922"
                            elif code.startswith("5"): color_map[code] = "#f85149"
                            else: color_map[code] = "#8b949e"

                        fig = px.bar(
                            status_data, x="Status Code", y="Count",
                            color="Status Code", color_discrete_map=color_map,
                        )
                        fig.update_layout(
                            height=300, margin=dict(l=10, r=10, t=10, b=10),
                            template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False,
                        )
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No status data yet.")

            with col_s2:
                st.subheader("Top Hosts")
                if len(agg_df) > 0:
                    top_hosts = agg_df.iloc[-1].get("top_hosts", [])
                    if top_hosts and isinstance(top_hosts, list):
                        hosts_df = pd.DataFrame(top_hosts)
                        if not hosts_df.empty:
                            hosts_df = hosts_df.rename(columns={"count": "Requests", "host": "Host"}, errors="ignore")
                            st.dataframe(hosts_df, use_container_width=True, hide_index=True)

            with col_s3:
                st.subheader("Top Paths")
                if len(agg_df) > 0:
                    top_paths = agg_df.iloc[-1].get("top_paths", [])
                    if top_paths and isinstance(top_paths, list):
                        paths_df = pd.DataFrame(top_paths)
                        if not paths_df.empty:
                            paths_df = paths_df.rename(columns={"count": "Requests", "path": "Path"}, errors="ignore")
                            st.dataframe(paths_df, use_container_width=True, hide_index=True)


# ─── Tab 2: Batch Analysis ──────────────────────────────────────────
with tab_batch:
    sub_anomaly, sub_rplot = st.tabs(["Anomaly Comparison", "Analytics Model (R/Python)"])
    
    with sub_anomaly:
        if os.path.exists(raw_access_log):
            st.markdown("<p style='color:#8b949e;'>Computing batch data directly from log file for anomaly comparison (Lines 5000-7000 vs Rest)</p>", unsafe_allow_html=True)
            
            @st.cache_data
            def compute_batch_anomaly_stats(filepath):
                normal_status = {}
                anomaly_status = {}
                normal_reqs = 0
                anomaly_reqs = 0
                normal_errors = 0
                anomaly_errors = 0
                
                with open(filepath, 'r') as f:
                    for idx, line in enumerate(f):
                        is_anomaly = 5000 <= idx <= 7000
                        parts = line.split('"')
                        if len(parts) >= 3:
                            status_part = parts[2].strip().split(' ')
                            if len(status_part) >= 1 and status_part[0].isdigit():
                                status = status_part[0]
                                is_err = status.startswith('4') or status.startswith('5')
                                if is_anomaly:
                                    anomaly_reqs += 1
                                    anomaly_status[status] = anomaly_status.get(status, 0) + 1
                                    if is_err: anomaly_errors += 1
                                else:
                                    normal_reqs += 1
                                    normal_status[status] = normal_status.get(status, 0) + 1
                                    if is_err: normal_errors += 1
                return {
                    "normal": {"reqs": normal_reqs, "errs": normal_errors, "status": normal_status},
                    "anomaly": {"reqs": anomaly_reqs, "errs": anomaly_errors, "status": anomaly_status}
                }

            stats = compute_batch_anomaly_stats(raw_access_log)
            
            b_c1, b_c2 = st.columns(2)
            with b_c1:
                st.subheader("Normal Traffic Segment")
                st.metric("Total Requests", f"{stats['normal']['reqs']:,}")
                n_err_rt = (stats['normal']['errs'] / max(1, stats['normal']['reqs'])) * 100
                st.metric("Error Rate", f"{n_err_rt:.2f}%")
                
                std_n = pd.DataFrame([{"Status": k, "Count": v} for k,v in stats['normal']['status'].items()])
                if not std_n.empty:
                    std_n = std_n.sort_values("Count", ascending=False).head(5)
                    fig_n = px.bar(std_n, x="Status", y="Count", color="Status")
                    fig_n.update_layout(height=250, margin=dict(l=10, r=10, t=10, b=10), template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_n, use_container_width=True)

            with b_c2:
                st.subheader("Anomaly Window (Lines 5000-7000)")
                st.metric("Total Requests", f"{stats['anomaly']['reqs']:,}")
                a_err_rt = (stats['anomaly']['errs'] / max(1, stats['anomaly']['reqs'])) * 100
                st.metric("Error Rate", f"{a_err_rt:.2f}%")
                
                std_a = pd.DataFrame([{"Status": k, "Count": v} for k,v in stats['anomaly']['status'].items()])
                if not std_a.empty:
                    std_a = std_a.sort_values("Count", ascending=False).head(5)
                    fig_a = px.bar(std_a, x="Status", y="Count", color="Status")
                    fig_a.update_layout(height=250, margin=dict(l=10, r=10, t=10, b=10), template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_a, use_container_width=True)
        else:
            st.error(f"Cannot find raw log at {raw_access_log}")

    with sub_rplot:
        analytics_dir = os.path.join(base_dir, "analytics")
        r_plots = [
            ("traffic_timeseries.png", "Traffic Over Time with Anomalies"),
            ("error_rate.png", "Error Rate Over Time"),
            ("status_distribution.png", "Status Code Distribution (R)"),
        ]

        plot_cols = st.columns(len(r_plots))
        r_plots_found = False
        for i, (filename, title) in enumerate(r_plots):
            filepath = os.path.join(analytics_dir, filename)
            if os.path.exists(filepath):
                with plot_cols[i]:
                    st.image(filepath, caption=title, use_container_width=True)
                r_plots_found = True

        if not r_plots_found:
            st.info("R plots not found at %s. Falling back to simple Python anomaly detection plot." % analytics_dir)
            if not agg_df.empty:
                df2 = agg_df.copy()
                df2['mean'] = df2['total_requests'].rolling(window=10, min_periods=1).mean()
                df2['std'] = df2['total_requests'].rolling(window=10, min_periods=1).std().fillna(0)
                df2['is_anomaly'] = df2['total_requests'] > (df2['mean'] + 2 * df2['std'])
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df2['window_id'], y=df2['total_requests'], mode='lines', name='Requests', line=dict(color="#2f81f7")))
                fig.add_trace(go.Scatter(x=df2['window_id'], y=df2['mean'], mode='lines', name='Rolling Mean', line=dict(dash='dash', color="#8b949e")))
                
                anomalies = df2[df2['is_anomaly']]
                fig.add_trace(go.Scatter(
                    x=anomalies['window_id'], y=anomalies['total_requests'],
                    mode='markers', name='Anomalies (>2 std)',
                    marker=dict(color='#f85149', size=8, symbol='x')
                ))
                
                fig.update_layout(title="Anomaly detection (Python fallback)", height=350, margin=dict(l=10, r=10, t=40, b=10), template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)

# ─── Auto-Refresh ───────────────────────────────────────────
st.markdown("<hr style='margin-top:1.5rem; margin-bottom:1rem;'/>", unsafe_allow_html=True)
auto_refresh = st.checkbox("Auto-refresh (every 5 seconds)", value=True)
if auto_refresh:
    time.sleep(5)
    st.rerun()

