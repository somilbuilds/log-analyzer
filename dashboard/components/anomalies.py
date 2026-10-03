"""Anomalies component — live anomaly detection feed."""

import streamlit as st
import pandas as pd
from dashboard.theme import COLORS


def render(agg_df: pd.DataFrame):
    st.markdown('<div class="section-title">Anomalies</div>', unsafe_allow_html=True)

    if agg_df.empty:
        st.markdown(
            '<div class="no-anomalies">No data available</div>',
            unsafe_allow_html=True,
        )
        return

    df = agg_df.copy()
    anomalies = []

    # Traffic spike detection: > mean + 2*std
    if len(df) >= 3:
        mean_req = df["total_requests"].mean()
        std_req = df["total_requests"].std()
        if std_req > 0:
            spikes = df[df["total_requests"] > (mean_req + 2 * std_req)]
            for _, row in spikes.iterrows():
                anomalies.append({
                    "window": int(row["window_id"]),
                    "type": "Traffic spike",
                    "metric": f"{int(row['total_requests']):,} requests",
                    "detail": f"threshold {int(mean_req + 2 * std_req):,}",
                    "severity": "critical",
                    "time": row.get("window_time", ""),
                })

    # Error rate spikes: > 10%
    high_err = df[df["error_rate"] > 0.10]
    for _, row in high_err.iterrows():
        anomalies.append({
            "window": int(row["window_id"]),
            "type": "Error surge",
            "metric": f"{row['error_rate'] * 100:.1f}% error rate",
            "detail": "threshold 10%",
            "severity": "warning",
            "time": row.get("window_time", ""),
        })

    # DDoS pattern: abnormally high request count in a single window
    if len(df) >= 5:
        p95 = df["total_requests"].quantile(0.95)
        ddos = df[df["total_requests"] > max(p95 * 1.5, mean_req + 3 * std_req if std_req > 0 else float("inf"))]
        for _, row in ddos.iterrows():
            # Avoid duplicates with traffic spikes
            wid = int(row["window_id"])
            if not any(a["window"] == wid and a["type"] == "Traffic spike" for a in anomalies):
                anomalies.append({
                    "window": wid,
                    "type": "DDoS detected",
                    "metric": f"{int(row['total_requests']):,} requests/window",
                    "detail": f"p95 = {int(p95):,}",
                    "severity": "critical",
                    "time": row.get("window_time", ""),
                })

    if not anomalies:
        st.markdown(
            '<div class="no-anomalies">No active anomalies</div>',
            unsafe_allow_html=True,
        )
        return

    # Sort by window descending (most recent first), limit to 10
    anomalies.sort(key=lambda a: a["window"], reverse=True)
    anomalies = anomalies[:10]

    for a in anomalies:
        css_class = "anomaly-card" if a["severity"] == "critical" else "anomaly-card warning"
        time_str = ""
        if a["time"]:
            try:
                time_str = a["time"].split("T")[1][:8]
            except (IndexError, AttributeError):
                time_str = str(a["time"])

        st.markdown(f"""
        <div class="{css_class}">
            <span class="anomaly-type">{a["type"]}</span>
            <span style="color:{COLORS['text_dim']}; margin:0 0.5rem;">—</span>
            <span class="anomaly-detail">{a["metric"]}</span>
            <span style="float:right; font-size:0.7rem; color:{COLORS['text_dim']};">
                W{a['window']} · {time_str}
            </span>
        </div>
        """, unsafe_allow_html=True)
