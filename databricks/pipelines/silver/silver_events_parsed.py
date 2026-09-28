"""
Silver: parsed GSI event envelopes.
`silver.gsi_events_parsed` — one row per event, deduplicated.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cs2.catalog")
BRONZE_TABLE = f"{CATALOG}.bronze.gsi_raw_events"

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


@dp.table(
    name=f"{CATALOG}.silver.gsi_events_parsed",
    comment="GSI event envelope parsed from raw JSON and deduplicated.",
    cluster_by=["match_id", "event_type"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(
    {
        "valid_json": "event_id IS NOT NULL",
        "known_match": "match_id IS NOT NULL",
        "known_player": "steamid IS NOT NULL",
        "known_event_type": (
            "event_type IN ('tick', 'round_start', 'round_end', 'match_start', 'match_end')"
        ),
        "known_time": "client_ts IS NOT NULL",
    }
)
def silver_gsi_events_parsed():
    envelope = F.from_json(F.col("raw_payload"), ENVELOPE_SCHEMA)
    return (
        spark.readStream.table(BRONZE_TABLE)
        .withColumn("envelope", envelope)
        .select(
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
        .withWatermark("client_ts", "2 hours")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
