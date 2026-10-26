"""Unity Catalog reads through the app's SQL warehouse, radar images from the bronze volume, and map thumbnails."""

import os
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from PIL import Image

# The bundle sets CS2_CATALOG per target. Locally it comes from the shell.
CATALOG = os.getenv("CS2_CATALOG", "cs2_dev")
GOLD = f"{CATALOG}.gold"

DIM_MATCH = f"{GOLD}.dim_match"
DIM_ROUND = f"{GOLD}.dim_round"
AGG_MATCH_SUMMARY = f"{GOLD}.agg_match_summary"
FACT_ROUND_STATS = f"{GOLD}.fact_round_stats"
FACT_PLAYER_POSITIONS = f"{GOLD}.fact_player_positions"
FACT_KILLS = f"{GOLD}.fact_kills"
FACT_DEATHS = f"{GOLD}.fact_deaths"
FACT_DAMAGE = f"{GOLD}.fact_damage"
DIM_MAP_REFERENCE = f"{GOLD}.dim_map_reference"
DIM_MAP_LEVEL = f"{GOLD}.dim_map_level"
DIM_WEAPON_SPEC = f"{GOLD}.dim_weapon_spec"


@st.cache_resource
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


def _connect():
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=600, show_spinner=False)
def query(statement: str, params: dict | None = None) -> pd.DataFrame:
    """Runs one statement with named `:markers`. A connection is not shared across Streamlit threads."""
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(statement, params or {})
        return cursor.fetchall_arrow().to_pandas()


def latest_match() -> pd.Series | None:
    """Most recently finished match, read past the cache."""
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT match_id, map_name, match_result, rounds_won, rounds_lost, ended_at
            FROM {DIM_MATCH}
            ORDER BY ended_at DESC
            LIMIT 1
            """
        )
        rows = cursor.fetchall_arrow().to_pandas()
    return None if rows.empty else rows.iloc[0]


@st.cache_data(show_spinner=False)
def radar_image(map_name: str, radar_urls: tuple[str, ...]) -> Image.Image | None:
    """Radar for the map's last listed level. The volume copy comes first, the source URL second."""
    if not radar_urls:
        return None
    target_url = radar_urls[-1]
    levels = query(
        f"SELECT radar_url, radar_volume_path FROM {DIM_MAP_LEVEL} WHERE map_name = :map_name",
        {"map_name": map_name},
    )
    match = levels[levels["radar_url"] == target_url]["radar_volume_path"].dropna()
    if not match.empty:
        try:
            content = workspace().files.download(match.iloc[0]).contents.read()
            return Image.open(BytesIO(content))
        except Exception:
            pass
    return _download_image(target_url)


@st.cache_data(show_spinner=False)
def thumbnail_image(thumb_urls: tuple[str, ...]) -> Image.Image | None:
    """First listed map thumbnail. The reference job keeps no volume copy, so it comes from the source URL."""
    return _download_image(thumb_urls[0]) if thumb_urls else None


def _download_image(url: str) -> Image.Image | None:
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return Image.open(BytesIO(resp.content))
    except Exception:
        pass
    return None
