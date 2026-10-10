"""CS2 Tracker app entry point: the home page starts jobs, the analysis page shows a match."""

import streamlit as st

st.set_page_config(layout="wide", page_title="CS2 Telemetry & Tactical Analytics")

HOME = st.Page("views/home.py", title="Home", default=True)
ANALYSIS = st.Page("views/analysis.py", title="Analysis")

st.navigation([HOME, ANALYSIS]).run()
