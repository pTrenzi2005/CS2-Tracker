"""
Silver: player ticks from the active round.
`silver.player_ticks` — one row per tick.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cs2.catalog")
PARSED_TABLE = f"{CATALOG}.silver.gsi_events_parsed"


def coordinate(column, index):
    return F.trim(F.split(F.col(column), ",").getItem(index)).cast("double")


@dp.table(
    name=f"{CATALOG}.silver.player_ticks",
    comment="Our player's state sampled during the active round.",
    cluster_by=["match_id", "round_number"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(
    {
        "non_negative_round": "round_number >= 0",
        "health_in_range": "health BETWEEN 0 AND 100",
        "non_negative_damage": "round_totaldmg >= 0",
        "known_position": "pos_x IS NOT NULL AND pos_y IS NOT NULL AND pos_z IS NOT NULL",
        "known_team": "player_team IN ('CT', 'T')",
    }
)
def silver_player_ticks():
    return (
        spark.readStream.table(PARSED_TABLE)
        .filter(F.col("event_type") == "tick")
        .select(
            "event_id",
            "match_id",
            "session_id",
            "seq",
            "client_ts",
            "provider_ts",
            "steamid",
            "round_number",
            F.col("payload.map.name").alias("map_name"),
            F.col("payload.map.mode").alias("map_mode"),
            F.col("payload.map.phase").alias("map_phase"),
            F.col("payload.round.phase").alias("round_phase"),
            F.col("payload.map.team_ct.score").alias("score_ct"),
            F.col("payload.map.team_t.score").alias("score_t"),
            F.col("payload.map.team_ct.consecutive_round_losses").alias("loss_streak_ct"),
            F.col("payload.map.team_t.consecutive_round_losses").alias("loss_streak_t"),
            F.col("payload.player.name").alias("player_name"),
            F.col("payload.player.team").alias("player_team"),
            F.col("payload.player.observer_slot").alias("observer_slot"),
            F.col("payload.player.state.health").alias("health"),
            F.col("payload.player.state.armor").alias("armor"),
            F.col("payload.player.state.helmet").alias("helmet"),
            F.col("payload.player.state.flashed").alias("flashed"),
            F.col("payload.player.state.smoked").alias("smoked"),
            F.col("payload.player.state.burning").alias("burning"),
            F.col("payload.player.state.money").alias("money"),
            F.col("payload.player.state.round_kills").alias("round_kills"),
            F.col("payload.player.state.round_killhs").alias("round_killhs"),
            F.col("payload.player.state.round_totaldmg").alias("round_totaldmg"),
            F.col("payload.player.state.equip_value").alias("equip_value"),
            F.col("payload.player.match_stats.kills").alias("match_kills"),
            F.col("payload.player.match_stats.assists").alias("match_assists"),
            F.col("payload.player.match_stats.deaths").alias("match_deaths"),
            F.col("payload.player.match_stats.mvps").alias("match_mvps"),
            F.col("payload.player.match_stats.score").alias("match_score"),
            F.col("payload.player.active_weapon").alias("active_weapon"),
            F.col("payload.player.weapons").alias("weapons"),
            F.col("payload.bomb.state").alias("bomb_state"),
            F.col("payload.phase_countdowns.phase_ends_in").cast("double").alias("phase_ends_in"),
            F.col("payload.player.position").alias("position_raw"),
            F.col("payload.player.forward").alias("forward_raw"),
            "ingest_ts",
        )
        .withColumn("pos_x", coordinate("position_raw", 0))
        .withColumn("pos_y", coordinate("position_raw", 1))
        .withColumn("pos_z", coordinate("position_raw", 2))
        .withColumn("fwd_x", coordinate("forward_raw", 0))
        .withColumn("fwd_y", coordinate("forward_raw", 1))
        .withColumn("fwd_z", coordinate("forward_raw", 2))
        .withColumn("yaw_deg", F.degrees(F.atan2(F.col("fwd_y"), F.col("fwd_x"))))
        .withColumn("pitch_deg", F.degrees(F.asin(-F.col("fwd_z"))))
        .withColumn("is_alive", F.col("health") > 0)
        .drop("position_raw", "forward_raw")
    )
