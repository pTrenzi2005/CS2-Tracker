"""
Silver: events derived from tick deltas.
GSI sends state, not kill or damage events. These are materialized views because `lag()` needs the full partition.
- `silver.kill_events` — ticks where the kill counter increased
- `silver.death_events` — one death per round
- `silver.damage_events` — damage dealt and taken
- `silver.round_player_state` — tick summary per round
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("cs2.catalog")
TICKS_TABLE = f"{CATALOG}.silver.player_ticks"
ROUND_EVENTS_TABLE = f"{CATALOG}.silver.round_events"

BY_ROUND = Window.partitionBy("match_id", "round_number").orderBy("seq")


def ticks_with_deltas():
    return (
        spark.read.table(TICKS_TABLE)
        .withColumn("prev_round_kills", F.lag("round_kills").over(BY_ROUND))
        .withColumn("prev_round_killhs", F.lag("round_killhs").over(BY_ROUND))
        .withColumn("prev_round_totaldmg", F.lag("round_totaldmg").over(BY_ROUND))
        .withColumn("prev_health", F.lag("health").over(BY_ROUND))
        .withColumn("prev_pos_x", F.lag("pos_x").over(BY_ROUND))
        .withColumn("prev_pos_y", F.lag("pos_y").over(BY_ROUND))
        .withColumn("prev_pos_z", F.lag("pos_z").over(BY_ROUND))
        # A match-level window would attach a last-second kill to the next round's spawn.
        # fact_round_stats adds those kills from round_end.
        .withColumn("prev_match_kills", F.lag("match_kills").over(BY_ROUND))
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.kill_events",
    comment="Ticks where our player's kill counter increased.",
    table_properties={"quality": "silver"},
)
def silver_kill_events():
    kills_in_round = F.col("round_kills") - F.coalesce(
        F.col("prev_round_kills"), F.col("round_kills")
    )
    kills_in_match = F.col("match_kills") - F.coalesce(
        F.col("prev_match_kills"), F.col("match_kills")
    )
    headshots = F.col("round_killhs") - F.coalesce(
        F.col("prev_round_killhs"), F.col("round_killhs")
    )
    return (
        ticks_with_deltas()
        .withColumn("kill_count", F.greatest(kills_in_round, kills_in_match))
        .withColumn("headshot_count", F.greatest(headshots, F.lit(0)))
        .filter(F.col("kill_count") > 0)
        .select(
            "event_id",
            "match_id",
            "round_number",
            "seq",
            "client_ts",
            "steamid",
            "map_name",
            "player_team",
            "kill_count",
            "headshot_count",
            F.col("active_weapon").alias("weapon_name"),
            "pos_x",
            "pos_y",
            "pos_z",
            "yaw_deg",
            "health",
            "round_totaldmg",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.death_events",
    comment="Player death in a round, from ticks and from round end.",
    table_properties={"quality": "silver"},
)
def silver_death_events():
    deaths_from_ticks = (
        ticks_with_deltas()
        .filter((F.col("health") == 0) & (F.coalesce(F.col("prev_health"), F.lit(100)) > 0))
        .select(
            "event_id",
            "match_id",
            "round_number",
            "seq",
            "client_ts",
            "steamid",
            "map_name",
            "player_team",
            F.col("active_weapon").alias("weapon_held"),
            "pos_x",
            "pos_y",
            "pos_z",
            F.col("prev_health").alias("health_before_death"),
            F.lit("tick").alias("source"),
        )
    )
    # A death on the round boundary may never produce a tick.
    deaths_from_round_end = (
        spark.read.table(ROUND_EVENTS_TABLE)
        .filter((F.col("event_type") == "round_end") & (F.col("player_survived") == F.lit(False)))
        .select(
            "event_id",
            "match_id",
            "round_number",
            "seq",
            "client_ts",
            "steamid",
            "map_name",
            "player_team",
            F.col("active_weapon").alias("weapon_held"),
            "pos_x",
            "pos_y",
            "pos_z",
            F.lit(None).cast("int").alias("health_before_death"),
            F.lit("round_end").alias("source"),
        )
    )
    dedup = Window.partitionBy("match_id", "round_number").orderBy(
        F.when(F.col("source") == "tick", 0).otherwise(1), F.col("seq")
    )
    return (
        deaths_from_ticks.unionByName(deaths_from_round_end)
        # At most one death per round. The tick is more precise than round_end.
        .withColumn("_rank", F.row_number().over(dedup))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.damage_events",
    comment="Damage dealt and taken, derived from state deltas.",
    table_properties={"quality": "silver"},
)
def silver_damage_events():
    deltas = ticks_with_deltas()
    common = [
        "event_id",
        "match_id",
        "round_number",
        "seq",
        "client_ts",
        "steamid",
        "map_name",
        "player_team",
        "pos_x",
        "pos_y",
        "pos_z",
    ]
    dealt = (
        deltas.withColumn(
            "damage",
            F.col("round_totaldmg")
            - F.coalesce(F.col("prev_round_totaldmg"), F.col("round_totaldmg")),
        )
        .filter(F.col("damage") > 0)
        .select(
            *common,
            F.col("active_weapon").alias("weapon_name"),
            F.lit("dealt").alias("damage_type"),
            "damage",
        )
    )
    
    taken = (
        deltas.withColumn("damage", F.col("prev_health") - F.col("health"))
        .filter(F.col("prev_health").isNotNull() & (F.col("damage") > 0))
        .select(
            *common,
            F.col("active_weapon").alias("weapon_name"),
            F.lit("taken").alias("damage_type"),
            "damage",
        )
    )
    return dealt.unionByName(taken)


@dp.materialized_view(
    name=f"{CATALOG}.silver.round_player_state",
    comment="Tick summary for a round: time, distance, and economy.",
    table_properties={"quality": "silver"},
)
def silver_round_player_state():
    step = F.sqrt(
        F.pow(F.col("pos_x") - F.col("prev_pos_x"), 2)
        + F.pow(F.col("pos_y") - F.col("prev_pos_y"), 2)
        + F.pow(F.col("pos_z") - F.col("prev_pos_z"), 2)
    )
    return (
        ticks_with_deltas()
        .withColumn("step_distance", F.coalesce(step, F.lit(0.0)))
        .groupBy("match_id", "round_number")
        .agg(
            F.expr("min_by(map_name, seq)").alias("map_name"),
            F.expr("min_by(steamid, seq)").alias("steamid"),
            F.expr("min_by(player_team, seq)").alias("player_team"),
            F.min("client_ts").alias("first_tick_ts"),
            F.max("client_ts").alias("last_tick_ts"),
            F.count("*").alias("tick_count"),
            F.max("round_kills").alias("round_kills"),
            F.max("round_killhs").alias("round_killhs"),
            F.max("round_totaldmg").alias("round_damage"),
            F.min("health").alias("min_health"),
            F.expr("min_by(money, seq)").alias("money_start"),
            F.expr("max_by(money, seq)").alias("money_end"),
            F.expr("min_by(equip_value, seq)").alias("equip_value_start"),
            F.max("equip_value").alias("equip_value_max"),
            F.expr("min_by(pos_x, seq)").alias("spawn_pos_x"),
            F.expr("min_by(pos_y, seq)").alias("spawn_pos_y"),
            F.round(F.sum("step_distance"), 2).alias("distance_units"),
        )
        .withColumn(
            "seconds_tracked",
            F.round(
                F.col("last_tick_ts").cast("double") - F.col("first_tick_ts").cast("double"), 2
            ),
        )
    )
