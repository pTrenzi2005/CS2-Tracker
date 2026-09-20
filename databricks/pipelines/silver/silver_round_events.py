"""Silver: zdarzenia cyklu zycia rundy i meczu.

Strumien niskiej czestotliwosci, ale bez niego warstwa Gold nie zna zwyciezcy
rundy: ``win_team`` pojawia sie w GSI wylacznie w fazie ``over``, ktora nie
trafia do ticku, a bywaja rundy, w ktorych zaden pakiet z ta faza nie dotyczy
juz naszego gracza.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cs2.catalog")
PARSED_TABLE = f"{CATALOG}.silver.gsi_events_parsed"


def coordinate(column, index):
    return F.trim(F.split(F.col(column), ",").getItem(index)).cast("double")


@dp.table(
    name=f"{CATALOG}.silver.round_events",
    comment="Poczatki i konce rund wraz ze zwyciezca oraz stanem gracza.",
    cluster_by=["match_id", "round_number"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({"numer_rundy_nieujemny": "round_number >= 0"})
@dp.expect_all({"koniec_rundy_ma_zwyciezce": "event_type <> 'round_end' OR win_team IN ('CT', 'T')"})
def silver_round_events():
    return (
        spark.readStream.table(PARSED_TABLE)
        .filter(F.col("event_type").isin("round_start", "round_end"))
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


@dp.table(
    name=f"{CATALOG}.silver.match_events",
    comment="Otwarcie i zamkniecie meczu wykryte po stronie klienta GSI.",
    cluster_by=["match_id"],
    table_properties={"quality": "silver"},
)
def silver_match_events():
    return (
        spark.readStream.table(PARSED_TABLE)
        .filter(F.col("event_type").isin("match_start", "match_end"))
        .select(
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
    )
