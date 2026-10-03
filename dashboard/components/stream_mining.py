"""Stream Mining component — Bloom Filter, DGIM, Flajolet-Martin panels."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.theme import COLORS, chart_layout


def _stat_row(label: str, value: str) -> str:
    return f"""
    <div class="stat-row">
        <span class="stat-label">{label}</span>
        <span class="stat-value">{value}</span>
    </div>"""


def render(bloom_df: pd.DataFrame, dgim_df: pd.DataFrame, fm_df: pd.DataFrame):
    st.markdown('<div class="section-title">Stream Mining</div>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)

    # ── Bloom Filter ────────────────────────────────────────────
    with c1:
        st.markdown('<span class="algo-badge">Approximate Streaming Algorithm</span>', unsafe_allow_html=True)
        st.markdown(f"**Bloom Filter**", help="Probabilistic set membership — tracks new vs previously seen hosts")

        if bloom_df.empty:
            st.markdown(f'<div class="no-anomalies">No data</div>', unsafe_allow_html=True)
        else:
            latest = bloom_df.iloc[-1]
            new_h = int(latest.get("new_hosts", 0))
            seen_h = int(latest.get("seen_hosts", 0))
            fp_rate = latest.get("estimated_fp_rate", 0)
            mem = int(latest.get("filter_memory_bytes", 0))
            total_items = int(latest.get("total_items_in_filter", 0))

            stats_html = (
                _stat_row("New hosts (window)", f"{new_h:,}")
                + _stat_row("Seen hosts (window)", f"{seen_h:,}")
                + _stat_row("Total in filter", f"{total_items:,}")
                + _stat_row("False-positive rate", f"{fp_rate:.4%}")
                + _stat_row("Memory", f"{mem:,} B")
            )
            st.markdown(f'<div style="background:{COLORS["surface"]};border:1px solid {COLORS["border"]};border-radius:6px;padding:0.6rem 0.8rem;margin-bottom:0.5rem;">{stats_html}</div>', unsafe_allow_html=True)

            # Mini chart
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=bloom_df["window_id"], y=bloom_df["new_hosts"],
                name="New", marker_color=COLORS["green"],
            ))
            fig.add_trace(go.Bar(
                x=bloom_df["window_id"], y=bloom_df["seen_hosts"],
                name="Seen", marker_color=COLORS["text_dim"],
            ))
            fig.update_layout(**chart_layout(
                height=180,
                barmode="stack",
                xaxis_title="Window",
            ))
            st.plotly_chart(fig, use_container_width=True)

    # ── DGIM ────────────────────────────────────────────────────
    with c2:
        st.markdown('<span class="algo-badge">Approximate Streaming Algorithm</span>', unsafe_allow_html=True)
        st.markdown("**DGIM**", help="Sliding-window approximate count of 5xx errors using O(log²N) memory")

        if dgim_df.empty:
            st.markdown(f'<div class="no-anomalies">No data</div>', unsafe_allow_html=True)
        else:
            latest = dgim_df.iloc[-1]
            approx = int(latest.get("dgim_approximate_5xx", 0))
            exact = int(latest.get("exact_5xx_total", 0))
            error_pct = abs(approx - exact) / max(exact, 1) * 100 if exact > 0 else 0
            buckets = int(latest.get("dgim_num_buckets", 0))
            win_size = int(latest.get("dgim_window_size", 0))

            stats_html = (
                _stat_row("Estimated 5xx", f"{approx:,}")
                + _stat_row("Exact 5xx", f"{exact:,}")
                + _stat_row("Approximation error", f"{error_pct:.1f}%")
                + _stat_row("Buckets", str(buckets))
                + _stat_row("Window size", f"{win_size:,}")
            )
            st.markdown(f'<div style="background:{COLORS["surface"]};border:1px solid {COLORS["border"]};border-radius:6px;padding:0.6rem 0.8rem;margin-bottom:0.5rem;">{stats_html}</div>', unsafe_allow_html=True)

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=dgim_df["window_id"], y=dgim_df["dgim_approximate_5xx"],
                mode="lines", name="DGIM Approx",
                line=dict(color=COLORS["amber"], width=1.5, dash="dash"),
                hovertemplate="Approx: %{y:,}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=dgim_df["window_id"], y=dgim_df["exact_5xx_total"],
                mode="lines", name="Exact",
                line=dict(color=COLORS["blue"], width=1.5),
                hovertemplate="Exact: %{y:,}<extra></extra>",
            ))
            fig.update_layout(**chart_layout(
                height=180,
                xaxis_title="Window",
            ))
            st.plotly_chart(fig, use_container_width=True)

    # ── Flajolet-Martin ─────────────────────────────────────────
    with c3:
        st.markdown('<span class="algo-badge">Approximate Streaming Algorithm</span>', unsafe_allow_html=True)
        st.markdown("**Flajolet-Martin**", help="Distinct host count estimation using O(log n) memory")

        if fm_df.empty:
            st.markdown(f'<div class="no-anomalies">No data</div>', unsafe_allow_html=True)
        else:
            latest = fm_df.iloc[-1]
            est = int(latest.get("fm_estimated_distinct_hosts", 0))
            exact = int(latest.get("exact_distinct_hosts", 0))
            err = float(latest.get("fm_error_pct", 0))

            stats_html = (
                _stat_row("Estimated distinct", f"{est:,}")
                + _stat_row("Exact distinct", f"{exact:,}")
                + _stat_row("Estimation error", f"{err:.1f}%")
            )
            st.markdown(f'<div style="background:{COLORS["surface"]};border:1px solid {COLORS["border"]};border-radius:6px;padding:0.6rem 0.8rem;margin-bottom:0.5rem;">{stats_html}</div>', unsafe_allow_html=True)

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=fm_df["window_id"], y=fm_df["fm_estimated_distinct_hosts"],
                mode="lines", name="FM Estimate",
                line=dict(color=COLORS["purple"], width=1.5, dash="dot"),
                hovertemplate="Estimate: %{y:,}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=fm_df["window_id"], y=fm_df["exact_distinct_hosts"],
                mode="lines", name="Exact",
                line=dict(color=COLORS["green"], width=1.5),
                hovertemplate="Exact: %{y:,}<extra></extra>",
            ))
            fig.update_layout(**chart_layout(
                height=180,
                xaxis_title="Window",
            ))
            st.plotly_chart(fig, use_container_width=True)
