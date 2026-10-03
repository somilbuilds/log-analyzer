"""Traffic component — primary time-series visualization."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.theme import COLORS, chart_layout


def render(agg_df: pd.DataFrame):
    st.markdown('<div class="section-title">Traffic</div>', unsafe_allow_html=True)

    if agg_df.empty:
        return

    # Metric selector
    metric_col, _ = st.columns([2, 10])
    with metric_col:
        metric = st.selectbox(
            "Metric",
            ["Request Volume", "Bytes Transferred"],
            label_visibility="collapsed",
        )

    y_col = "total_requests" if metric == "Request Volume" else "total_bytes"
    y_label = "Requests" if metric == "Request Volume" else "Bytes"

    # Compute rolling mean and anomaly threshold
    df = agg_df.copy()
    df["rolling_mean"] = df[y_col].rolling(window=10, min_periods=1).mean()
    df["rolling_std"] = df[y_col].rolling(window=10, min_periods=1).std().fillna(0)
    df["is_anomaly"] = df[y_col] > (df["rolling_mean"] + 2 * df["rolling_std"])

    fig = go.Figure()

    # Main trace
    fig.add_trace(go.Scatter(
        x=df["window_id"], y=df[y_col],
        mode="lines",
        name=y_label,
        line=dict(color=COLORS["blue"], width=1.5),
        fill="tozeroy",
        fillcolor=f"{COLORS['blue']}12",
        hovertemplate=f"Window %{{x}}<br>{y_label}: %{{y:,.0f}}<extra></extra>",
    ))

    # Rolling mean
    fig.add_trace(go.Scatter(
        x=df["window_id"], y=df["rolling_mean"],
        mode="lines",
        name="Rolling Mean",
        line=dict(color=COLORS["text_dim"], width=1, dash="dot"),
        hovertemplate="Mean: %{y:,.0f}<extra></extra>",
    ))

    # Anomaly markers
    anomalies = df[df["is_anomaly"]]
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies["window_id"], y=anomalies[y_col],
            mode="markers",
            name="Anomaly",
            marker=dict(color=COLORS["red"], size=7, symbol="diamond"),
            hovertemplate="ANOMALY — Window %{x}<br>Value: %{y:,.0f}<extra></extra>",
        ))

    fig.update_layout(**chart_layout(
        height=340,
        xaxis_title="Window",
        yaxis_title=y_label,
    ))
    st.plotly_chart(fig, use_container_width=True)

    # Secondary: error rate trend (compact)
    fig_err = go.Figure()
    fig_err.add_trace(go.Scatter(
        x=df["window_id"], y=df["error_rate"] * 100,
        mode="lines",
        name="Error Rate %",
        line=dict(color=COLORS["red"], width=1.5),
        fill="tozeroy",
        fillcolor=f"{COLORS['red']}10",
        hovertemplate="Window %{x}<br>Error Rate: %{y:.2f}%<extra></extra>",
    ))

    # Mark high-error windows
    high_err = df[df["error_rate"] > 0.10]
    if not high_err.empty:
        fig_err.add_trace(go.Scatter(
            x=high_err["window_id"], y=high_err["error_rate"] * 100,
            mode="markers",
            name="High Error",
            marker=dict(color=COLORS["amber"], size=5, symbol="diamond"),
            hovertemplate="HIGH — %{y:.2f}%<extra></extra>",
        ))

    fig_err.update_layout(**chart_layout(
        height=160,
        xaxis_title="Window",
        yaxis_title="Error %",
    ))
    st.plotly_chart(fig_err, use_container_width=True)
