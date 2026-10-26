"""
Silver: events derived from tick deltas.
GSI sends state, not kill or damage events. These are materialized views because `lag()` needs the full partition.
- `silver.kill_events` — ticks where the kill counter increased
- `silver.death_events` — one death per round
- `silver.damage_events` — damage dealt and taken
- `silver.round_player_state` — tick summary per round
"""

from pyspark import pipelines as dp

from cs2_transforms import silver

CATALOG = spark.conf.get("cs2.catalog")
TICKS_TABLE = f"{CATALOG}.silver.player_ticks"
ROUND_EVENTS_TABLE = f"{CATALOG}.silver.round_events"


@dp.materialized_view(
    name=f"{CATALOG}.silver.kill_events",
    comment="Ticks where our player's kill counter increased.",
    table_properties={"quality": "silver"},
)
def silver_kill_events():
    return silver.kill_events(spark.read.table(TICKS_TABLE))


@dp.materialized_view(
    name=f"{CATALOG}.silver.death_events",
    comment="Player death in a round, from ticks and from round end.",
    table_properties={"quality": "silver"},
)
def silver_death_events():
    return silver.death_events(
        spark.read.table(TICKS_TABLE), spark.read.table(ROUND_EVENTS_TABLE)
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.damage_events",
    comment="Damage dealt and taken, derived from state deltas.",
    table_properties={"quality": "silver"},
)
def silver_damage_events():
    return silver.damage_events(spark.read.table(TICKS_TABLE))


@dp.materialized_view(
    name=f"{CATALOG}.silver.round_player_state",
    comment="Tick summary for a round: time, distance, and economy.",
    table_properties={"quality": "silver"},
)
def silver_round_player_state():
    return silver.round_player_state(spark.read.table(TICKS_TABLE))
