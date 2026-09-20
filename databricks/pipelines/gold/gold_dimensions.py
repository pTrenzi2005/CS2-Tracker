"""Gold: wymiary schematu gwiazdy.

Klucze surogatowe liczymy funkcjami skrotu z kluczy naturalnych, wiec sa
stabilne przy kazdym pelnym przeliczeniu widokow i nie wymagaja sekwencerow.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cs2.catalog")
TICKS = f"{CATALOG}.silver.player_ticks"
ROUND_EVENTS = f"{CATALOG}.silver.round_events"
MATCH_EVENTS = f"{CATALOG}.silver.match_events"
ROUND_STATE = f"{CATALOG}.silver.round_player_state"


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_player",
    comment="Gracz z zamaskowanym SteamID64 zgodnie z polityka prywatnosci.",
    table_properties={"quality": "gold"},
)
def gold_dim_player():
    return (
        spark.read.table(TICKS)
        .groupBy("steamid")
        .agg(
            F.expr("max_by(player_name, seq)").alias("player_name"),
            F.min("client_ts").alias("first_seen_ts"),
            F.max("client_ts").alias("last_seen_ts"),
            F.countDistinct("match_id").alias("matches_played"),
        )
        .select(
            F.sha2(F.col("steamid"), 256).alias("player_key"),
            # Surowy SteamID64 nie opuszcza warstwy Silver.
            F.concat(F.substring(F.col("steamid"), 1, 8), F.lit("*********")).alias(
                "steamid_masked"
            ),
            "player_name",
            "first_seen_ts",
            "last_seen_ts",
            "matches_played",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_map",
    comment="Mapy widziane w telemetrii; punkt podpiecia metadanych wsadowych.",
    table_properties={"quality": "gold"},
)
def gold_dim_map():
    return (
        spark.read.table(TICKS)
        .groupBy("map_name")
        .agg(
            F.countDistinct("match_id").alias("matches_played"),
            F.min("client_ts").alias("first_seen_ts"),
        )
        .select(
            F.xxhash64(F.col("map_name")).alias("map_key"),
            "map_name",
            "matches_played",
            "first_seen_ts",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_weapon",
    comment="Bronie widziane w ekwipunku; punkt podpiecia cennika i statystyk.",
    table_properties={"quality": "gold"},
)
def gold_dim_weapon():
    weapon_class = (
        F.when(F.col("weapon_type").isin("Knife", "Melee", "Fists"), "melee")
        .when(F.col("weapon_type") == "Pistol", "sidearm")
        .when(
            F.col("weapon_type").isin("Rifle", "SniperRifle", "Submachine Gun", "Shotgun", "Machine Gun"),
            "primary",
        )
        .when(F.col("weapon_type") == "Grenade", "utility")
        .when(F.col("weapon_type") == "C4", "objective")
        .otherwise("other")
    )
    return (
        spark.read.table(TICKS)
        .select(F.explode("weapons").alias("weapon"))
        .select(
            F.col("weapon.name").alias("weapon_name"),
            F.col("weapon.type").alias("weapon_type"),
        )
        .filter(F.col("weapon_name").isNotNull())
        .distinct()
        .select(
            F.xxhash64(F.col("weapon_name")).alias("weapon_key"),
            "weapon_name",
            "weapon_type",
            weapon_class.alias("weapon_class"),
            (F.col("weapon_type") == "Grenade").alias("is_grenade"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_round",
    comment="Runda z wynikiem, ekonomia startowa i czasem trwania.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_dim_round():
    events = spark.read.table(ROUND_EVENTS)
    starts = (
        events.filter(F.col("event_type") == "round_start")
        .groupBy("match_id", "round_number")
        .agg(
            F.min("client_ts").alias("round_start_ts"),
            F.expr("min_by(player_team, seq)").alias("team_at_start"),
            F.expr("min_by(equip_value, seq)").alias("equip_value_start"),
            F.expr("min_by(money, seq)").alias("money_start"),
            F.expr("min_by(map_name, seq)").alias("map_name_start"),
            F.expr("min_by(map_mode, seq)").alias("map_mode"),
            F.expr("min_by(steamid, seq)").alias("steamid_start"),
        )
    )
    ends = (
        events.filter(F.col("event_type") == "round_end")
        .groupBy("match_id", "round_number")
        .agg(
            F.max("client_ts").alias("round_end_ts"),
            F.expr("max_by(win_team, seq)").alias("win_team"),
            F.expr("max_by(win_reason, seq)").alias("win_reason"),
            F.expr("max_by(player_survived, seq)").alias("player_survived"),
            F.expr("max_by(player_team, seq)").alias("team_at_end"),
            F.expr("max_by(score_ct, seq)").alias("score_ct_after"),
            F.expr("max_by(score_t, seq)").alias("score_t_after"),
            F.expr("max_by(map_name, seq)").alias("map_name_end"),
            F.expr("max_by(steamid, seq)").alias("steamid_end"),
        )
    )
    state = spark.read.table(ROUND_STATE).select(
        "match_id", "round_number", F.col("player_team").alias("team_from_ticks")
    )

    player_team = F.coalesce("team_at_start", "team_at_end", "team_from_ticks")
    equip = F.col("equip_value_start")
    return (
        starts.join(ends, ["match_id", "round_number"], "full_outer")
        .join(state, ["match_id", "round_number"], "left")
        .withColumn("player_team", player_team)
        .withColumn("map_name", F.coalesce("map_name_start", "map_name_end"))
        .select(
            F.xxhash64(F.col("match_id"), F.col("round_number")).alias("round_key"),
            F.xxhash64(F.col("match_id")).alias("match_key"),
            F.xxhash64(F.col("map_name")).alias("map_key"),
            F.sha2(F.coalesce("steamid_start", "steamid_end"), 256).alias("player_key"),
            "match_id",
            "round_number",
            "map_name",
            "map_mode",
            "round_start_ts",
            "round_end_ts",
            F.round(
                F.col("round_end_ts").cast("double") - F.col("round_start_ts").cast("double"), 2
            ).alias("round_duration_s"),
            "player_team",
            "win_team",
            "win_reason",
            (F.col("win_team") == player_team).alias("player_won"),
            F.coalesce(F.col("player_survived"), F.lit(False)).alias("player_survived"),
            "money_start",
            F.col("equip_value_start").alias("equip_value_start"),
            F.when(equip.isNull(), "unknown")
            .when(equip < 2000, "eco")
            .when(equip < 3500, "force")
            .otherwise("full")
            .alias("round_type"),
            # Zalozenie formatu MR12: pistolowki otwieraja obie polowy.
            F.col("round_number").isin(0, 12).alias("is_pistol_round"),
            "score_ct_after",
            "score_t_after",
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_match",
    comment="Mecz z bilansem rund naszego gracza.",
    table_properties={"quality": "gold"},
)
def gold_dim_match():
    rounds = spark.read.table(f"{CATALOG}.gold.dim_round")
    per_match = rounds.groupBy("match_id").agg(
        F.count("*").alias("rounds_played"),
        F.sum(F.when(F.col("player_won"), 1).otherwise(0)).alias("rounds_won"),
        F.sum(F.when(F.col("player_survived"), 1).otherwise(0)).alias("rounds_survived"),
        F.min("round_start_ts").alias("first_round_ts"),
        F.max("round_end_ts").alias("last_round_ts"),
        F.expr("max_by(score_ct_after, round_number)").alias("final_score_ct"),
        F.expr("max_by(score_t_after, round_number)").alias("final_score_t"),
        F.expr("min_by(map_name, round_number)").alias("map_name"),
        F.expr("min_by(map_mode, round_number)").alias("map_mode"),
        F.expr("min_by(player_key, round_number)").alias("player_key"),
    )
    events = spark.read.table(MATCH_EVENTS)
    boundaries = events.groupBy("match_id").agg(
        F.min(F.when(F.col("event_type") == "match_start", F.col("client_ts"))).alias("started_at"),
        F.max(F.when(F.col("event_type") == "match_end", F.col("client_ts"))).alias("ended_at"),
    )
    rounds_lost = F.col("rounds_played") - F.col("rounds_won")
    return (
        per_match.join(boundaries, "match_id", "left")
        .withColumn("started_at", F.coalesce("started_at", "first_round_ts"))
        .withColumn("ended_at", F.coalesce("ended_at", "last_round_ts"))
        .select(
            F.xxhash64(F.col("match_id")).alias("match_key"),
            F.xxhash64(F.col("map_name")).alias("map_key"),
            "player_key",
            "match_id",
            "map_name",
            "map_mode",
            "started_at",
            "ended_at",
            F.round(
                F.col("ended_at").cast("double") - F.col("started_at").cast("double"), 2
            ).alias("match_duration_s"),
            "rounds_played",
            "rounds_won",
            rounds_lost.alias("rounds_lost"),
            "rounds_survived",
            "final_score_ct",
            "final_score_t",
            F.when(F.col("rounds_won") > rounds_lost, "win")
            .when(F.col("rounds_won") < rounds_lost, "loss")
            .otherwise("draw")
            .alias("match_result"),
        )
    )
