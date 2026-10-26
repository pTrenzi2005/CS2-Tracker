"""Silver: parsed envelopes, ticks, round and match events, and events derived from tick deltas."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

WEAPON = (
    "STRUCT<slot: INT, name: STRING, type: STRING, state: STRING, "
    "ammo_clip: INT, ammo_clip_max: INT, ammo_reserve: INT>"
)
TEAM = (
    "STRUCT<score: INT, consecutive_round_losses: INT, "
    "timeouts_remaining: INT, matches_won_this_series: INT>"
)
PLAYER_STATE = (
    "STRUCT<health: INT, armor: INT, helmet: BOOLEAN, flashed: INT, smoked: INT, "
    "burning: INT, money: INT, round_kills: INT, round_killhs: INT, "
    "round_totaldmg: INT, equip_value: INT>"
)
MATCH_STATS = "STRUCT<kills: INT, assists: INT, deaths: INT, mvps: INT, score: INT>"
PLAYER = (
    f"STRUCT<name: STRING, team: STRING, activity: STRING, observer_slot: INT, "
    f"state: {PLAYER_STATE}, match_stats: {MATCH_STATS}, weapons: ARRAY<{WEAPON}>, "
    f"active_weapon: STRING, position: STRING, forward: STRING>"
)
MAP = (
    f"STRUCT<name: STRING, mode: STRING, phase: STRING, round: INT, "
    f"num_matches_to_win_series: INT, team_ct: {TEAM}, team_t: {TEAM}>"
)
PAYLOAD = (
    f"STRUCT<map: {MAP}, "
    "round: STRUCT<phase: STRING, win_team: STRING, bomb: STRING>, "
    "bomb: STRUCT<state: STRING, position: STRING, player: STRING, countdown: STRING>, "
    "phase_countdowns: STRUCT<phase: STRING, phase_ends_in: STRING>, "
    f"player: {PLAYER}, "
    "phase: STRING, win_team: STRING, win_reason: STRING, player_survived: BOOLEAN, "
    "rounds_played: INT, started_at_provider_ts: BIGINT>"
)
ENVELOPE_SCHEMA = (
    "schema_version STRING, event_type STRING, event_id STRING, session_id STRING, "
    "match_id STRING, seq BIGINT, client_ts STRING, provider_ts BIGINT, "
    f"steamid STRING, round_number INT, payload {PAYLOAD}"
)

EVENT_RULES = {
    "valid_json": "event_id IS NOT NULL",
    "known_match": "match_id IS NOT NULL",
    "known_player": "steamid IS NOT NULL",
    "known_event_type": (
        "event_type IN ('tick', 'round_start', 'round_end', 'match_start', 'match_end')"
    ),
    "known_time": "client_ts IS NOT NULL",
}

TICK_RULES = {
    "non_negative_round": "round_number >= 0",
    "health_in_range": "health BETWEEN 0 AND 100",
    "non_negative_damage": "round_totaldmg >= 0",
    "known_position": "pos_x IS NOT NULL AND pos_y IS NOT NULL AND pos_z IS NOT NULL",
    "known_team": "player_team IN ('CT', 'T')",
}

ROUND_EVENT_RULES = {"non_negative_round": "round_number >= 0"}
ROUND_EVENT_WARNINGS = {
    "round_end_has_winner": "event_type <> 'round_end' OR win_team IN ('CT', 'T')"
}


def parse_envelopes(raw: DataFrame) -> DataFrame:
    """Bronze rows to one typed envelope per row. Deduplication stays with the caller."""
    envelope = F.from_json(F.col("raw_payload"), ENVELOPE_SCHEMA)
    return raw.withColumn("envelope", envelope).select(
        F.col("envelope.event_id").alias("event_id"),
        F.col("envelope.event_type").alias("event_type"),
        F.col("envelope.schema_version").alias("schema_version"),
        F.col("envelope.session_id").alias("session_id"),
        F.col("envelope.match_id").alias("match_id"),
        F.col("envelope.seq").alias("seq"),
        F.col("envelope.client_ts").cast("timestamp").alias("client_ts"),
        F.col("envelope.provider_ts").cast("timestamp").alias("provider_ts"),
        F.col("envelope.steamid").alias("steamid"),
        F.col("envelope.round_number").alias("round_number"),
        F.col("envelope.payload").alias("payload"),
        F.col("ingest_ts"),
        F.col("kafka_offset"),
    )


def coordinate(column: str, index: int) -> Column:
    """One component of a GSI vector string such as "-512.00, 1024.50, 64.03"."""
    return F.trim(F.split(F.col(column), ",").getItem(index)).cast("double")


def player_ticks(parsed: DataFrame) -> DataFrame:
    return (
        parsed.filter(F.col("event_type") == "tick")
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


def round_events(parsed: DataFrame) -> DataFrame:
    return (
        parsed.filter(F.col("event_type").isin("round_start", "round_end"))
        .select(
            "event_id",
            "event_type",
            "match_id",
            "session_id",
            "seq",
            "client_ts",
            "provider_ts",
            "steamid",
            "round_number",
            F.col("payload.map.name").alias("map_name"),
            F.col("payload.map.mode").alias("map_mode"),
            F.col("payload.map.team_ct.score").alias("score_ct"),
            F.col("payload.map.team_t.score").alias("score_t"),
            F.col("payload.win_team").alias("win_team"),
            F.col("payload.win_reason").alias("win_reason"),
            F.col("payload.player_survived").alias("player_survived"),
            F.col("payload.player.team").alias("player_team"),
            F.col("payload.player.state.health").alias("health"),
            F.col("payload.player.state.money").alias("money"),
            F.col("payload.player.state.equip_value").alias("equip_value"),
            F.col("payload.player.state.round_kills").alias("round_kills"),
            F.col("payload.player.state.round_killhs").alias("round_killhs"),
            F.col("payload.player.state.round_totaldmg").alias("round_totaldmg"),
            F.col("payload.player.match_stats.deaths").alias("match_deaths"),
            F.col("payload.player.active_weapon").alias("active_weapon"),
            F.col("payload.player.position").alias("position_raw"),
            "ingest_ts",
        )
        .withColumn("pos_x", coordinate("position_raw", 0))
        .withColumn("pos_y", coordinate("position_raw", 1))
        .withColumn("pos_z", coordinate("position_raw", 2))
        .drop("position_raw")
    )


def match_events(parsed: DataFrame) -> DataFrame:
    return parsed.filter(F.col("event_type").isin("match_start", "match_end")).select(
        "event_id",
        "event_type",
        "match_id",
        "session_id",
        "seq",
        "client_ts",
        "provider_ts",
        "steamid",
        F.col("payload.map.name").alias("map_name"),
        F.col("payload.map.mode").alias("map_mode"),
        F.col("payload.map.team_ct.score").alias("score_ct"),
        F.col("payload.map.team_t.score").alias("score_t"),
        F.col("payload.rounds_played").alias("rounds_played"),
        "ingest_ts",
    )


def with_deltas(ticks: DataFrame) -> DataFrame:
    by_round = Window.partitionBy("match_id", "round_number").orderBy("seq")
    return (
        ticks.withColumn("prev_round_kills", F.lag("round_kills").over(by_round))
        .withColumn("prev_round_killhs", F.lag("round_killhs").over(by_round))
        .withColumn("prev_round_totaldmg", F.lag("round_totaldmg").over(by_round))
        .withColumn("prev_health", F.lag("health").over(by_round))
        .withColumn("prev_pos_x", F.lag("pos_x").over(by_round))
        .withColumn("prev_pos_y", F.lag("pos_y").over(by_round))
        .withColumn("prev_pos_z", F.lag("pos_z").over(by_round))
        # A match-level window would attach a last-second kill to the next round's spawn.
        # fact_round_stats adds those kills from round_end.
        .withColumn("prev_match_kills", F.lag("match_kills").over(by_round))
    )


def kill_events(ticks: DataFrame) -> DataFrame:
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
        with_deltas(ticks)
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


def death_events(ticks: DataFrame, rounds: DataFrame) -> DataFrame:
    deaths_from_ticks = (
        with_deltas(ticks)
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
    deaths_from_round_end = rounds.filter(
        (F.col("event_type") == "round_end") & (F.col("player_survived") == F.lit(False))
    ).select(
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


def damage_events(ticks: DataFrame) -> DataFrame:
    deltas = with_deltas(ticks)
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


def round_player_state(ticks: DataFrame) -> DataFrame:
    step = F.sqrt(
        F.pow(F.col("pos_x") - F.col("prev_pos_x"), 2)
        + F.pow(F.col("pos_y") - F.col("prev_pos_y"), 2)
        + F.pow(F.col("pos_z") - F.col("prev_pos_z"), 2)
    )
    return (
        with_deltas(ticks)
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
