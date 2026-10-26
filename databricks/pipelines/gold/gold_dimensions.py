"""
Gold: star-schema dimensions.
Keys are hashes of natural keys, so full recomputes stay stable.
- `gold.dim_player` — one row per player
- `gold.dim_map` — one row per map seen in telemetry
- `gold.dim_weapon` — one row per weapon seen in inventory
- `gold.dim_round` — one row per round
- `gold.dim_match` — one row per match
"""

from pyspark import pipelines as dp

from cs2_transforms import gold

CATALOG = spark.conf.get("cs2.catalog")
TICKS = f"{CATALOG}.silver.player_ticks"
ROUND_EVENTS = f"{CATALOG}.silver.round_events"
MATCH_EVENTS = f"{CATALOG}.silver.match_events"
ROUND_STATE = f"{CATALOG}.silver.round_player_state"


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_player",
    comment="Player with SteamID64 masked.",
    table_properties={"quality": "gold"},
)
def gold_dim_player():
    return gold.dim_player(spark.read.table(TICKS))


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_map",
    comment="Maps seen in telemetry. Join point for reference metadata.",
    table_properties={"quality": "gold"},
)
def gold_dim_map():
    return gold.dim_map(spark.read.table(TICKS))


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_weapon",
    comment="Weapons seen in inventory. Join point for price and stats.",
    table_properties={"quality": "gold"},
)
def gold_dim_weapon():
    return gold.dim_weapon(spark.read.table(TICKS))


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_round",
    comment="Round with result, starting economy, and duration.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_dim_round():
    return gold.dim_round(spark.read.table(ROUND_EVENTS), spark.read.table(ROUND_STATE))


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_match",
    comment="Match with our player's round record.",
    table_properties={"quality": "gold"},
)
def gold_dim_match():
    return gold.dim_match(
        spark.read.table(f"{CATALOG}.gold.dim_round"), spark.read.table(MATCH_EVENTS)
    )
