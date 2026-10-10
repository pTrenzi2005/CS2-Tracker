"""Home: run the reference batch and the match analysis, then open the newest match."""

import pandas as pd
import streamlit as st

import data
import runs

POLL_S = 5
ANALYSIS_PAGE = "views/analysis.py"

if st.session_state.pop("open_analysis", False):
    st.switch_page(ANALYSIS_PAGE)


def show_run(run: runs.RunStatus | None, job_id: str | None) -> None:
    if not job_id:
        st.caption("Job id is not set. Deploy the app with the bundle.")
    elif run is None:
        st.caption("No runs yet.")
    else:
        link = f" · [open run]({run.url})" if run.url else ""
        st.caption(f"Last run {run.run_id}: **{run.state}**{link}")


def settle(batch: runs.RunStatus | None, analysis: runs.RunStatus | None) -> bool:
    """Follows up on runs this session waits for. True when one of them has finished."""
    settled = False
    if batch and not batch.active and st.session_state.get("pending_batch") == batch.run_id:
        del st.session_state["pending_batch"]
        settled = True
    if analysis and not analysis.active and st.session_state.get("pending_analysis") == analysis.run_id:
        del st.session_state["pending_analysis"]
        settled = True
        if not analysis.succeeded:
            st.session_state.home_notice = f"Analysis run {analysis.run_id} ended with {analysis.state}."
        elif (match := data.latest_match()) is None:
            st.session_state.home_notice = "Analysis finished, but gold.dim_match has no matches yet."
        else:
            st.session_state.focus_match = match["match_id"]
            st.session_state.open_analysis = True
    if settled:
        st.cache_data.clear()
    return settled


batch_run = runs.latest(runs.REFERENCE_JOB_ID)
analysis_run = runs.latest(runs.ANALYSIS_JOB_ID)

# A reload during a run keeps the follow-up: an active run becomes this session's.
if batch_run and batch_run.active:
    st.session_state.pending_batch = batch_run.run_id
if analysis_run and analysis_run.active:
    st.session_state.pending_analysis = analysis_run.run_id

if settle(batch_run, analysis_run):
    st.rerun()

st.title("CS2 Tracker")
st.caption(
    "The GSI server runs on the gaming PC (`python gsi_server.py` in `python/`) "
    "and sends match events to Event Hubs while you play."
)

if notice := st.session_state.pop("home_notice", None):
    st.warning(notice)

col_batch, col_analysis = st.columns(2)

with col_batch:
    st.subheader("Reference data")
    st.write("Maps, radars, and weapon stats into gold. Run once, then again after a game update.")
    if st.button(
        "RUN BATCH",
        disabled=not runs.REFERENCE_JOB_ID or bool(batch_run and batch_run.active),
    ):
        st.session_state.pending_batch = runs.start(runs.REFERENCE_JOB_ID)
        st.rerun()

with col_analysis:
    st.subheader("Match analysis")
    st.write(
        "Reads new events from Event Hubs through bronze, silver, and gold, "
        "then opens the newest match."
    )
    if st.button(
        "RUN ANALYSIS",
        type="primary",
        disabled=not runs.ANALYSIS_JOB_ID or bool(analysis_run and analysis_run.active),
    ):
        st.session_state.pending_analysis = runs.start(runs.ANALYSIS_JOB_ID)
        st.rerun()

polling = "pending_batch" in st.session_state or "pending_analysis" in st.session_state


@st.fragment(run_every=POLL_S if polling else None)
def run_status() -> None:
    batch = runs.latest(runs.REFERENCE_JOB_ID)
    analysis = runs.latest(runs.ANALYSIS_JOB_ID)
    col_b, col_a = st.columns(2)
    with col_b:
        show_run(batch, runs.REFERENCE_JOB_ID)
    with col_a:
        show_run(analysis, runs.ANALYSIS_JOB_ID)
    if settle(batch, analysis):
        st.rerun()


run_status()

st.divider()
st.subheader("Latest match")
latest = data.latest_match()
if latest is None:
    st.info("No matches in gold yet. Play a match with the GSI server running, then RUN ANALYSIS.")
else:
    c1, c2, c3 = st.columns(3)
    c1.metric("Map", latest["map_name"])
    c2.metric("Result", f"{str(latest['match_result']).upper()} {latest['rounds_won']}-{latest['rounds_lost']}")
    ended_at = latest["ended_at"]
    c3.metric("Ended", f"{ended_at:%Y-%m-%d %H:%M}" if pd.notna(ended_at) else "unknown")
    if st.button("Open analysis"):
        st.session_state.focus_match = latest["match_id"]
        st.switch_page(ANALYSIS_PAGE)
