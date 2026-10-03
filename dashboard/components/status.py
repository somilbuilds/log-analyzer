"""HTTP Status component — status code distribution and trends."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.theme import COLORS, chart_layout


STATUS_COLORS = {
    "2": COLORS["green"],
    "3": COLORS["blue"],
    "4": COLORS["amber"],
    "5": COLORS["red"],
}


def _classify(code: str) -> str:
    if code.startswith("2"):
        return "2xx"
    elif code.startswith("3"):
        return "3xx"
    elif code.startswith("4"):
        return "4xx"
    elif code.startswith("5"):
        return "5xx"
    return "other"


def render(agg_df: pd.DataFrame):
    st.markdown('<div class="section-title">HTTP Status</div>', unsafe_allow_html=True)

    if agg_df.empty:
        return

    col_dist, col_trend = st.columns([1, 1])

    with col_dist:
        # Aggregate status codes across recent windows
        class_totals = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0}
        for _, row in agg_df.iterrows():
            dist = row.get("status_distribution", {})
            if isinstance(dist, dict):
                for code, count in dist.items():
                    cls = _classify(str(code))
                    if cls in class_totals:
                        class_totals[cls] += count

        categories = list(class_totals.keys())
        values = [class_totals[c] for c in categories]
        colors = [STATUS_COLORS.get(c[0], COLORS["text_muted"]) for c in categories]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=categories,
            x=values,
            orientation="h",
            marker_color=colors,
            hovertemplate="%{y}: %{x:,}<extra></extra>",
            text=[f"{v:,}" for v in values],
            textposition="auto",
            textfont=dict(size=11, family="JetBrains Mono, monospace", color=COLORS["text"]),
        ))
        fig.update_layout(**chart_layout(
            height=220,
            title="Status Distribution",
            xaxis_title="Count",
            yaxis=dict(
                gridcolor=COLORS["border"],
                categoryorder="array",
                categoryarray=["5xx", "4xx", "3xx", "2xx"],
            ),
            showlegend=False,
        ))
        st.plotly_chart(fig, use_container_width=True)

    with col_trend:
        # Per-window error rate trend
        if len(agg_df) > 1:
            # Build per-class time series from status_distribution
            records = []
            for _, row in agg_df.iterrows():
                wid = row["window_id"]
                dist = row.get("status_distribution", {})
                if isinstance(dist, dict):
                    total = sum(dist.values()) or 1
                    for cls_key in ["4", "5"]:
                        cls_count = sum(v for k, v in dist.items() if str(k).startswith(cls_key))
                        records.append({
                            "window_id": wid,
                            "class": f"{cls_key}xx",
                            "pct": cls_count / total * 100,
                        })

            if records:
                trend_df = pd.DataFrame(records)
                fig2 = go.Figure()
                for cls, color in [("4xx", COLORS["amber"]), ("5xx", COLORS["red"])]:
                    sub = trend_df[trend_df["class"] == cls]
                    if not sub.empty:
                        fig2.add_trace(go.Scatter(
                            x=sub["window_id"],
                            y=sub["pct"],
                            mode="lines",
                            name=cls,
                            line=dict(color=color, width=1.5),
                            hovertemplate=f"{cls}: %{{y:.1f}}%<extra></extra>",
                        ))
                fig2.update_layout(**chart_layout(
                    height=220,
                    title="Error Trend",
                    xaxis_title="Window",
                    yaxis_title="%",
                ))
                st.plotly_chart(fig2, use_container_width=True)
