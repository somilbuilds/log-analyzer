#!/usr/bin/env python3
"""
Log Analyzer — Real-Time Web Traffic Observability Dashboard
=============================================================

Modular Streamlit dashboard that reads from MongoDB collections
populated by the Spark/local streaming pipeline.

Usage:
    streamlit run dashboard/app.py
"""

import sys
import os
import time

# Ensure dashboard package is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from dashboard.theme import CSS
from dashboard.data.queries import (
    fetch_window_aggregates,
    fetch_bloom_stats,
    fetch_dgim_stats,
    fetch_fm_stats,
)
from dashboard.components import (
    header,
    controls,
    overview,
    traffic,
    status,
    anomalies,
    stream_mining,
    breakdown,
)
from dashboard.components.controls import apply_time_filter

# ─── Page Config ────────────────────────────────────────────────
st.set_page_config(
    page_title="Log Analyzer",
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Inject Theme CSS ──────────────────────────────────────────
st.markdown(CSS, unsafe_allow_html=True)

# ─── Fetch Data ─────────────────────────────────────────────────
agg_df = fetch_window_aggregates()
bloom_df = fetch_bloom_stats()
dgim_df = fetch_dgim_stats()
fm_df = fetch_fm_stats()

# ─── Render Sections ────────────────────────────────────────────
header.render()
controls.render()

# Apply time range filter
time_range = st.session_state.get("time_range", "All")
agg_df = apply_time_filter(agg_df, time_range)
bloom_df = apply_time_filter(bloom_df, time_range)
dgim_df = apply_time_filter(dgim_df, time_range)
fm_df = apply_time_filter(fm_df, time_range)

overview.render(agg_df, bloom_df)

tab1, tab2, tab3 = st.tabs(["🚀 Core Traffic & Anomalies", "📊 Status & Breakdown", "⚡ Stream Mining"])

with tab1:
    traffic.render(agg_df)
    anomalies.render(agg_df)

with tab2:
    status.render(agg_df)
    breakdown.render(agg_df)

with tab3:
    stream_mining.render(bloom_df, dgim_df, fm_df)

# ─── Auto-Refresh ──────────────────────────────────────────────
if st.session_state.get("auto_refresh", True):
    time.sleep(5)
    st.rerun()
