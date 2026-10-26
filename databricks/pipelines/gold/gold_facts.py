"""
Gold: star-schema facts.
- `gold.fact_player_positions` — one row per tick
- `gold.fact_kills` — one row per kill sample
- `gold.fact_deaths` — one row per death
- `gold.fact_damage` — one row per damage delta
- `gold.fact_round_stats` — one row per round and player
- `gold.agg_match_summary` — one row per match and player
"""

from pyspark import pipelines as dp

from cs2_transforms import gold

CATALOG = spark.conf.get("cs2.catalog")
TICKS = f"{CATALOG}.silver.player_ticks"
KILL_EVENTS = f"{CATALOG}.silver.kill_events"
DEATH_EVENTS = f"{CATALOG}.silver.death_events"
DAMAGE_EVENTS = f"{CATALOG}.silver.damage_events"
ROUND_STATE = f"{CATALOG}.silver.round_player_state"
ROUND_EVENTS = f"{CATALOG}.silver.round_events"
DIM_ROUND = f"{CATALOG}.gold.dim_round"
DIM_MATCH = f"{CATALOG}.gold.dim_match"
FACT_ROUND_STATS = f"{CATALOG}.gold.fact_round_stats"


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_player_positions",
    comment="Grain: tick. Player path in the round, with state and weapon.",
    cluster_by=["match_id", "round_number"],
    table_properties={"quality": "gold"},
)
def gold_fact_player_positions():
    return gold.fact_player_positions(spark.read.table(TICKS), spark.read.table(DIM_ROUND))


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_kills",
    comment="Grain: kill event. Place, weapon, and time in the round.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_kills():
    return gold.fact_kills(spark.read.table(KILL_EVENTS), spark.read.table(DIM_ROUND))


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_deaths",
    comment="Grain: player death in a round. Place and time alive.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_deaths():
    return gold.fact_deaths(spark.read.table(DEATH_EVENTS), spark.read.table(DIM_ROUND))


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_damage",
    comment="Grain: damage dealt or taken.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_damage():
    return gold.fact_damage(spark.read.table(DAMAGE_EVENTS), spark.read.table(DIM_ROUND))


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_round_stats",
    comment="Grain: round and player. Source for K/D, ADR, and survival rate.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_round_stats():
    return gold.fact_round_stats(
        spark.read.table(DIM_ROUND),
        spark.read.table(ROUND_STATE),
        spark.read.table(KILL_EVENTS),
        spark.read.table(DEATH_EVENTS),
        spark.read.table(DAMAGE_EVENTS),
        spark.read.table(ROUND_EVENTS),
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_match_summary",
    comment="Grain: match and player. Dashboard KPIs.",
    table_properties={"quality": "gold"},
)
def gold_agg_match_summary():
    return gold.agg_match_summary(spark.read.table(FACT_ROUND_STATS), spark.read.table(DIM_MATCH))
