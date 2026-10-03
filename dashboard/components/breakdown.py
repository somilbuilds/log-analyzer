"""Breakdown component — top endpoints, top hosts, request distribution."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.theme import COLORS, chart_layout


def render(agg_df: pd.DataFrame):
    st.markdown('<div class="section-title">Traffic Breakdown</div>', unsafe_allow_html=True)

    if agg_df.empty or len(agg_df) == 0:
        return

    # Aggregate top hosts and top paths across all windows
    host_totals = {}
    path_totals = {}

    for _, row in agg_df.iterrows():
        top_hosts = row.get("top_hosts", [])
        if isinstance(top_hosts, list):
            for entry in top_hosts:
                h = entry.get("host", "unknown")
                c = entry.get("count", 0)
                host_totals[h] = host_totals.get(h, 0) + c

        top_paths = row.get("top_paths", [])
        if isinstance(top_paths, list):
            for entry in top_paths:
                p = entry.get("path", "/")
                c = entry.get("count", 0)
                path_totals[p] = path_totals.get(p, 0) + c

    c1, c2 = st.columns(2)

    with c1:
        if host_totals:
            sorted_hosts = sorted(host_totals.items(), key=lambda x: -x[1])[:10]
            hosts = [h for h, _ in reversed(sorted_hosts)]
            counts = [c for _, c in reversed(sorted_hosts)]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=hosts, x=counts,
                orientation="h",
                marker_color=COLORS["cyan"],
                hovertemplate="%{y}<br>%{x:,} requests<extra></extra>",
                text=[f"{c:,}" for c in counts],
                textposition="auto",
                textfont=dict(size=10, family="JetBrains Mono, monospace", color=COLORS["text"]),
            ))
            fig.update_layout(**chart_layout(
                height=300,
                title="Top Hosts",
                xaxis_title="Requests",
                showlegend=False,
                margin=dict(l=0, r=0, t=28, b=0),
            ))
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        if path_totals:
            sorted_paths = sorted(path_totals.items(), key=lambda x: -x[1])[:10]
            paths = [p for p, _ in reversed(sorted_paths)]
            counts = [c for _, c in reversed(sorted_paths)]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=paths, x=counts,
                orientation="h",
                marker_color=COLORS["blue"],
                hovertemplate="%{y}<br>%{x:,} requests<extra></extra>",
                text=[f"{c:,}" for c in counts],
                textposition="auto",
                textfont=dict(size=10, family="JetBrains Mono, monospace", color=COLORS["text"]),
            ))
            fig.update_layout(**chart_layout(
                height=300,
                title="Top Endpoints",
                xaxis_title="Requests",
                showlegend=False,
                margin=dict(l=0, r=0, t=28, b=0),
            ))
            st.plotly_chart(fig, use_container_width=True)
