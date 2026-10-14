"""CS2 Tracker app entry point: the home page starts jobs, the analysis page shows a match."""

import streamlit as st

st.set_page_config(layout="wide", page_title="CS2 Match Telemetry Intelligence")

st.markdown("""
<style>
    div[data-testid="stMetric"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        padding: 12px 18px;
        border-radius: 10px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    }
    div[data-testid="stMetricLabel"] {
        color: #8b949e;
        font-size: 0.85rem;
    }
    div[data-testid="stMetricValue"] {
        color: #f0f6fc;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

HOME = st.Page("views/home.py", title="Home", default=True)
ANALYSIS = st.Page("views/analysis.py", title="Analysis")

st.navigation([HOME, ANALYSIS]).run()
