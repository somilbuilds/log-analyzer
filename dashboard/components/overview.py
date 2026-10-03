"""Overview component — compact KPI row."""

import streamlit as st
import pandas as pd
from dashboard.theme import COLORS


def render(agg_df: pd.DataFrame, bloom_df: pd.DataFrame):
    st.markdown('<div class="section-title">Overview</div>', unsafe_allow_html=True)

    if agg_df.empty:
        st.markdown(
            f'<div class="no-anomalies">No data yet — start the demo or wait for the pipeline.</div>',
            unsafe_allow_html=True,
        )
        return

    c1, c2, c3, c4, c5 = st.columns(5)

    total_requests = int(agg_df["total_requests"].sum())
    avg_error_rate = agg_df["error_rate"].mean() * 100

    # Compute deltas from last 2 windows
    delta_requests = None
    delta_error = None
    if len(agg_df) >= 2:
        last = agg_df.iloc[-1]
        prev = agg_df.iloc[-2]
        delta_requests = int(last["total_requests"] - prev["total_requests"])
        delta_error = round((last["error_rate"] - prev["error_rate"]) * 100, 2)

    with c1:
        st.metric("Requests", f"{total_requests:,}",
                   delta=f"{delta_requests:+,}" if delta_requests is not None else None)
    with c2:
        st.metric("Error Rate", f"{avg_error_rate:.2f}%",
                   delta=f"{delta_error:+.2f}%" if delta_error is not None else None,
                   delta_color="inverse")
    with c3:
        if not bloom_df.empty:
            unique = int(bloom_df.iloc[-1].get("total_items_in_filter", 0))
            st.metric("Unique Hosts", f"{unique:,}")
        else:
            st.metric("Unique Hosts", "—")
    with c4:
        total_bytes = int(agg_df["total_bytes"].sum()) if "total_bytes" in agg_df.columns else 0
        if total_bytes > 1_000_000:
            st.metric("Throughput", f"{total_bytes / 1_000_000:.1f} MB")
        elif total_bytes > 1_000:
            st.metric("Throughput", f"{total_bytes / 1_000:.1f} KB")
        else:
            st.metric("Throughput", f"{total_bytes:,} B")
    with c5:
        # Anomaly count: windows where requests > mean + 2*std
        anomaly_count = 0
        if len(agg_df) >= 3:
            mean = agg_df["total_requests"].mean()
            std = agg_df["total_requests"].std()
            if std > 0:
                anomaly_count = int((agg_df["total_requests"] > (mean + 2 * std)).sum())
            # Also count error spikes
            anomaly_count += int((agg_df["error_rate"] > 0.10).sum())
        color = COLORS["green"] if anomaly_count == 0 else COLORS["red"]
        st.metric("Active Anomalies", str(anomaly_count))

    st.markdown("<div style='height:0.5rem;'></div>", unsafe_allow_html=True)
