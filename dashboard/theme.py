"""
Theme — Dark observability design system.
Single source of truth for colors, CSS, chart defaults.
"""

# ── Color Palette (semantic) ────────────────────────────────────
COLORS = {
    "bg":           "#0b0e14",
    "surface":      "#131720",
    "surface_alt":  "#181d28",
    "border":       "#1e2530",
    "border_light": "#2a3140",
    "text":         "#e0e4ea",
    "text_muted":   "#7a8394",
    "text_dim":     "#505968",
    # Semantic
    "green":        "#2dd4a0",
    "green_bg":     "#2dd4a012",
    "amber":        "#f0b429",
    "amber_bg":     "#f0b42912",
    "red":          "#ef4444",
    "red_bg":       "#ef444412",
    "cyan":         "#22d3ee",
    "cyan_bg":      "#22d3ee12",
    "blue":         "#3b82f6",
    "blue_bg":      "#3b82f612",
    "purple":       "#a78bfa",
}

# ── CSS ──────────────────────────────────────────────────────────
CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Reset ──────────────────────────────────────────── */
.block-container {{
    padding-top: 1rem !important;
    padding-bottom: 0rem !important;
    max-width: 1280px !important;
}}
#MainMenu, footer, header {{visibility: hidden;}}

/* ── Base ───────────────────────────────────────────── */
html, body, .stApp {{
    background-color: {COLORS["bg"]} !important;
    color: {COLORS["text"]} !important;
    font-family: 'Inter', -apple-system, system-ui, sans-serif !important;
}}

/* ── Typography ─────────────────────────────────────── */
h1, h2, h3, h4, h5, h6 {{
    color: {COLORS["text"]} !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
}}
p, span, label, div {{
    font-family: 'Inter', sans-serif !important;
}}

/* ── Section titles ─────────────────────────────────── */
.section-title {{
    font-size: 0.7rem;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: {COLORS["text_muted"]};
    margin-bottom: 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid {COLORS["border"]};
}}

/* ── KPI Cards ──────────────────────────────────────── */
div[data-testid="metric-container"] {{
    background: {COLORS["surface"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
    padding: 0.9rem 0.75rem;
    text-align: left;
}}
div[data-testid="metric-container"] label {{
    color: {COLORS["text_muted"]} !important;
    font-size: 0.7rem !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-weight: 600;
}}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {{
    font-size: 1.5rem !important;
    font-weight: 700;
    color: {COLORS["text"]} !important;
    font-family: 'JetBrains Mono', monospace !important;
}}
div[data-testid="metric-container"] [data-testid="stMetricDelta"] {{
    font-size: 0.7rem !important;
}}

/* ── Plotly containers ──────────────────────────────── */
.stPlotlyChart {{
    background: {COLORS["surface"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
    padding: 0;
}}

/* ── Dataframes ─────────────────────────────────────── */
.stDataFrame {{
    background: {COLORS["surface"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
}}

/* ── Buttons ────────────────────────────────────────── */
.stButton > button {{
    background: {COLORS["surface_alt"]} !important;
    border: 1px solid {COLORS["border_light"]} !important;
    color: {COLORS["text"]} !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 500;
    border-radius: 4px;
    padding: 0.35rem 0.9rem;
    transition: background 0.15s;
}}
.stButton > button:hover {{
    background: {COLORS["border_light"]} !important;
}}

/* ── Selectbox / Checkbox ───────────────────────────── */
.stSelectbox label, .stCheckbox label, .stRadio label {{
    color: {COLORS["text_muted"]} !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}}
.stSelectbox [data-baseweb="select"] {{
    background: {COLORS["surface"]} !important;
    border-color: {COLORS["border"]} !important;
}}

/* ── Dividers ───────────────────────────────────────── */
hr {{
    border-color: {COLORS["border"]} !important;
    margin: 0.5rem 0 !important;
}}

/* ── Tabs styling ───────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {{
    gap: 0;
    background: {COLORS["surface"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
    padding: 0.2rem;
}}
.stTabs [data-baseweb="tab"] {{
    height: 2.2rem;
    background: transparent;
    border-radius: 4px;
    color: {COLORS["text_muted"]};
    font-size: 0.78rem;
    font-weight: 500;
}}
.stTabs [aria-selected="true"] {{
    background: {COLORS["surface_alt"]} !important;
    color: {COLORS["text"]} !important;
}}
.stTabs [data-baseweb="tab-highlight"] {{
    display: none;
}}

/* ── Expander ───────────────────────────────────────── */
.streamlit-expanderHeader {{
    background: {COLORS["surface"]} !important;
    border: 1px solid {COLORS["border"]} !important;
    border-radius: 6px;
    color: {COLORS["text_muted"]} !important;
    font-size: 0.78rem !important;
}}

/* ── Custom utility classes ─────────────────────────── */
.mono {{
    font-family: 'JetBrains Mono', monospace !important;
}}
.status-dot {{
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
}}
.status-dot.live {{
    background: {COLORS["green"]};
    box-shadow: 0 0 6px {COLORS["green"]}80;
}}
.status-dot.offline {{
    background: {COLORS["text_dim"]};
}}
.algo-badge {{
    display: inline-block;
    font-size: 0.6rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.15rem 0.45rem;
    border-radius: 3px;
    background: {COLORS["cyan_bg"]};
    color: {COLORS["cyan"]};
    border: 1px solid {COLORS["cyan"]}30;
    margin-bottom: 0.5rem;
}}
.anomaly-card {{
    background: {COLORS["surface"]};
    border: 1px solid {COLORS["border"]};
    border-left: 3px solid {COLORS["red"]};
    border-radius: 4px;
    padding: 0.6rem 0.8rem;
    margin-bottom: 0.4rem;
    font-size: 0.82rem;
}}
.anomaly-card.warning {{
    border-left-color: {COLORS["amber"]};
}}
.anomaly-card .anomaly-type {{
    font-weight: 600;
    color: {COLORS["text"]};
}}
.anomaly-card .anomaly-detail {{
    color: {COLORS["text_muted"]};
    font-size: 0.75rem;
    font-family: 'JetBrains Mono', monospace;
}}
.pipeline-step {{
    display: inline-block;
    font-size: 0.68rem;
    color: {COLORS["text_muted"]};
    padding: 0.15rem 0.5rem;
    border: 1px solid {COLORS["border"]};
    border-radius: 3px;
    background: {COLORS["surface"]};
}}
.pipeline-arrow {{
    display: inline-block;
    color: {COLORS["text_dim"]};
    font-size: 0.6rem;
    margin: 0 0.2rem;
}}
.stat-row {{
    display: flex;
    justify-content: space-between;
    padding: 0.35rem 0;
    border-bottom: 1px solid {COLORS["border"]};
    font-size: 0.82rem;
}}
.stat-row:last-child {{
    border-bottom: none;
}}
.stat-label {{
    color: {COLORS["text_muted"]};
}}
.stat-value {{
    color: {COLORS["text"]};
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
}}
.no-anomalies {{
    color: {COLORS["text_dim"]};
    font-size: 0.82rem;
    text-align: center;
    padding: 1.5rem;
    background: {COLORS["surface"]};
    border: 1px solid {COLORS["border"]};
    border-radius: 6px;
}}
</style>
"""


def chart_layout(height: int = 320, **overrides) -> dict:
    """Return consistent Plotly layout defaults."""
    layout = dict(
        height=height,
        margin=dict(l=0, r=0, t=28, b=0),
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=11, color=COLORS["text_muted"]),
        title_font=dict(size=12, color=COLORS["text_muted"]),
        xaxis=dict(
            gridcolor=COLORS["border"],
            zerolinecolor=COLORS["border"],
            showgrid=True,
            gridwidth=1,
        ),
        yaxis=dict(
            gridcolor=COLORS["border"],
            zerolinecolor=COLORS["border"],
            showgrid=True,
            gridwidth=1,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color=COLORS["text_muted"]),
            bgcolor="rgba(0,0,0,0)",
        ),
        hoverlabel=dict(
            bgcolor=COLORS["surface_alt"],
            bordercolor=COLORS["border_light"],
            font=dict(family="JetBrains Mono, monospace", size=11, color=COLORS["text"]),
        ),
    )
    layout.update(overrides)
    return layout
