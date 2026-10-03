"""Header component — LOG ANALYZER title, pipeline status, live indicator."""

import streamlit as st
from dashboard.data.queries import get_pipeline_status
from dashboard.theme import COLORS


def render():
    status = get_pipeline_status()

    dot_class = "live" if status["running"] else "offline"
    dot_label = "LIVE" if status["running"] else "OFFLINE"
    dot_color = COLORS["green"] if status["running"] else COLORS["text_dim"]

    last_update = status["last_update"] or "—"
    if last_update and last_update != "—":
        # Show only time portion
        try:
            last_update = last_update.split("T")[1][:8]
        except (IndexError, AttributeError):
            pass

    st.markdown(f"""
    <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:0.25rem;">
        <div>
            <span style="font-size:1.3rem; font-weight:700; color:{COLORS['text']}; letter-spacing:-0.02em;">LOG ANALYZER</span>
            <span style="font-size:0.75rem; color:{COLORS['text_muted']}; margin-left:0.75rem;">real-time web traffic observability</span>
        </div>
        <div style="display:flex; align-items:center; gap:1rem;">
            <span style="font-size:0.7rem; color:{COLORS['text_dim']};">
                Last update <span class="mono" style="color:{COLORS['text_muted']};">{last_update}</span>
            </span>
            <span style="font-size:0.68rem; font-weight:600; letter-spacing:0.06em; color:{dot_color};">
                <span class="status-dot {dot_class}"></span>{dot_label}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Pipeline status strip
    steps = ["Replayer", "Spark", "Analytics", "MongoDB"]
    pipeline_html = ""
    for i, step in enumerate(steps):
        pipeline_html += f'<span class="pipeline-step">{step}</span>'
        if i < len(steps) - 1:
            pipeline_html += '<span class="pipeline-arrow">→</span>'

    st.markdown(f"""
    <div style="margin-bottom:0.75rem; margin-top:0.15rem;">
        {pipeline_html}
        <span style="font-size:0.65rem; color:{COLORS['text_dim']}; margin-left:0.75rem;">
            {status['window_count']} windows processed
        </span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<hr/>", unsafe_allow_html=True)
