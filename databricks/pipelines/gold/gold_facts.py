"""Gold: tabele faktow schematu gwiazdy pod dashboardy analityczne.

``fact_player_positions`` ma ziarno pojedynczego ticku i sluzy do odtwarzania
sciezki gracza w rundzie. Pozostale fakty maja ziarno zdarzenia albo rundy.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cs2.catalog")
TICKS = f"{CATALOG}.silver.player_ticks"
KILL_EVENTS = f"{CATALOG}.silver.kill_events"
DEATH_EVENTS = f"{CATALOG}.silver.death_events"
DAMAGE_EVENTS = f"{CATALOG}.silver.damage_events"
ROUND_STATE = f"{CATALOG}.silver.round_player_state"
ROUND_EVENTS = f"{CATALOG}.silver.round_events"
DIM_ROUND = f"{CATALOG}.gold.dim_round"


def with_keys(df, weapon_column: str | None = None):
    out = (
        df.withColumn("match_key", F.xxhash64("match_id"))
        .withColumn("round_key", F.xxhash64(F.col("match_id"), F.col("round_number")))
        .withColumn("player_key", F.sha2(F.col("steamid"), 256))
        .withColumn("map_key", F.xxhash64("map_name"))
    )
    if weapon_column is not None:
        out = out.withColumn("weapon_key", F.xxhash64(F.col(weapon_column)))
    return out


def with_time_in_round(df):
    """Dokleja sekundy od poczatku rundy - os czasu dla kazdego dashboardu."""
    rounds = spark.read.table(DIM_ROUND).select("match_id", "round_number", "round_start_ts")
    return (
        df.join(rounds, ["match_id", "round_number"], "left")
        .withColumn(
            "time_in_round_s",
            F.round(
                F.col("client_ts").cast("double") - F.col("round_start_ts").cast("double"), 3
            ),
        )
        .drop("round_start_ts")
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_player_positions",
    comment="Ziarno: tick. Sciezka gracza w rundzie razem ze stanem i bronia.",
    cluster_by=["match_id", "round_number"],
    table_properties={"quality": "gold"},
)
def gold_fact_player_positions():
    ticks = spark.read.table(TICKS)
    return with_time_in_round(with_keys(ticks, "active_weapon")).select(
        "round_key",
        "match_key",
        "player_key",
        "map_key",
        "weapon_key",
        "match_id",
        "round_number",
        F.col("client_ts").alias("tick_ts"),
        "time_in_round_s",
        "seq",
        "pos_x",
        "pos_y",
        "pos_z",
        "yaw_deg",
        "pitch_deg",
        "health",
        "armor",
        "money",
        "equip_value",
        "is_alive",
        (F.col("flashed") > 0).alias("is_flashed"),
        (F.col("smoked") > 0).alias("is_smoked"),
        (F.col("burning") > 0).alias("is_burning"),
        F.col("active_weapon").alias("weapon_name"),
        "player_team",
        "bomb_state",
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_kills",
    comment="Ziarno: zdarzenie zabojstwa. Miejsce, bron i moment w rundzie.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_kills():
    kills = spark.read.table(KILL_EVENTS)
    return with_time_in_round(with_keys(kills, "weapon_name")).select(
        "round_key",
        "match_key",
        "player_key",
        "map_key",
        "weapon_key",
        "match_id",
        "round_number",
        F.col("client_ts").alias("kill_ts"),
        "time_in_round_s",
        "kill_count",
        "headshot_count",
        "weapon_name",
        "player_team",
        "pos_x",
        "pos_y",
        "pos_z",
        "yaw_deg",
        F.col("health").alias("health_at_kill"),
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_deaths",
    comment="Ziarno: smierc gracza w rundzie. Miejsce i przezyty czas.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_deaths():
    deaths = spark.read.table(DEATH_EVENTS)
    return with_time_in_round(with_keys(deaths, "weapon_held")).select(
        "round_key",
        "match_key",
        "player_key",
        "map_key",
        "weapon_key",
        "match_id",
        "round_number",
        F.col("client_ts").alias("death_ts"),
        F.col("time_in_round_s").alias("time_alive_s"),
        F.col("weapon_held").alias("weapon_name"),
        "player_team",
        "pos_x",
        "pos_y",
        "pos_z",
        "health_before_death",
        F.col("source").alias("detected_from"),
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_damage",
    comment="Ziarno: przyrost obrazen zadanych lub otrzymanych.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_damage():
    damage = spark.read.table(DAMAGE_EVENTS)
    return with_time_in_round(with_keys(damage, "weapon_name")).select(
        "round_key",
        "match_key",
        "player_key",
        "map_key",
        "weapon_key",
        "match_id",
        "round_number",
        F.col("client_ts").alias("damage_ts"),
        "time_in_round_s",
        "damage_type",
        "damage",
        "weapon_name",
        "player_team",
        "pos_x",
        "pos_y",
        "pos_z",
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_round_stats",
    comment="Ziarno: runda x gracz. Podstawa dla K/D, ADR i procentu przezyc.",
    cluster_by=["match_id"],
    table_properties={"quality": "gold"},
)
def gold_fact_round_stats():
    rounds = spark.read.table(DIM_ROUND)
    state = spark.read.table(ROUND_STATE).select(
        "match_id",
        "round_number",
        "round_kills",
        "round_killhs",
        "round_damage",
        "distance_units",
        "tick_count",
        "min_health",
        "equip_value_max",
    )
    kills = (
        spark.read.table(KILL_EVENTS)
        .groupBy("match_id", "round_number")
        .agg(
            F.sum("kill_count").alias("kills"),
            F.sum("headshot_count").alias("headshot_kills"),
        )
    )
    deaths = (
        spark.read.table(DEATH_EVENTS)
        .groupBy("match_id", "round_number")
        .agg(F.min("client_ts").alias("death_ts"))
    )
    damage = (
        spark.read.table(DAMAGE_EVENTS)
        .groupBy("match_id", "round_number")
        .agg(
            F.sum(F.when(F.col("damage_type") == "dealt", F.col("damage")).otherwise(0)).alias(
                "damage_dealt"
            ),
            F.sum(F.when(F.col("damage_type") == "taken", F.col("damage")).otherwise(0)).alias(
                "damage_taken"
            ),
        )
    )
    # Ticki urywaja sie w chwili smierci, bo kamera przechodzi na kolege z
    # druzyny. Liczniki ze zdarzenia round_end pochodza z ostatniego pakietu
    # rundy, wiec obejmuja rowniez to, co wydarzylo sie po ostatnim ticku, i to
    # one sa tu zrodlem prawdy dla sum.
    final_state = (
        spark.read.table(ROUND_EVENTS)
        .filter(F.col("event_type") == "round_end")
        .groupBy("match_id", "round_number")
        .agg(
            F.expr("max_by(round_kills, seq)").alias("final_kills"),
            F.expr("max_by(round_killhs, seq)").alias("final_headshots"),
            F.expr("max_by(round_totaldmg, seq)").alias("final_damage"),
        )
    )

    joined = (
        rounds.join(state, ["match_id", "round_number"], "left")
        .join(kills, ["match_id", "round_number"], "left")
        .join(deaths, ["match_id", "round_number"], "left")
        .join(damage, ["match_id", "round_number"], "left")
        .join(final_state, ["match_id", "round_number"], "left")
    )
    time_alive = F.round(
        F.col("death_ts").cast("double") - F.col("round_start_ts").cast("double"), 2
    )
    return joined.select(
        "round_key",
        "match_key",
        "player_key",
        "map_key",
        "match_id",
        "round_number",
        "player_team",
        "round_type",
        "is_pistol_round",
        "win_team",
        "player_won",
        "player_survived",
        "round_start_ts",
        "round_duration_s",
        F.coalesce(
            F.greatest(F.col("final_kills"), F.col("kills"), F.col("round_kills")), F.lit(0)
        ).alias("kills"),
        F.coalesce(
            F.greatest(F.col("final_headshots"), F.col("headshot_kills"), F.col("round_killhs")),
            F.lit(0),
        ).alias("headshot_kills"),
        # Zabojstwa, dla ktorych mamy tick, a wiec i pozycje na mapie. Roznica
        # wobec kolumny kills to zabojstwa z samej koncowki rundy.
        F.coalesce(F.col("kills"), F.lit(0)).alias("kills_with_position"),
        F.when(F.col("death_ts").isNotNull(), 1).otherwise(0).alias("deaths"),
        F.coalesce(
            F.greatest(F.col("final_damage"), F.col("damage_dealt"), F.col("round_damage")),
            F.lit(0),
        ).alias("damage_dealt"),
        F.coalesce(F.col("damage_taken"), F.lit(0)).alias("damage_taken"),
        F.coalesce(time_alive, F.col("round_duration_s")).alias("time_alive_s"),
        F.coalesce(F.col("distance_units"), F.lit(0.0)).alias("distance_units"),
        F.col("money_start").alias("money_start"),
        F.col("equip_value_start").alias("equip_value_start"),
        F.col("equip_value_max").alias("equip_value_max"),
        F.coalesce(F.col("tick_count"), F.lit(0)).alias("tick_count"),
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_match_summary",
    comment="Ziarno: mecz x gracz. Gotowe KPI na kafelki dashboardu.",
    table_properties={"quality": "gold"},
)
def gold_agg_match_summary():
    stats = spark.read.table(f"{CATALOG}.gold.fact_round_stats")
    matches = spark.read.table(f"{CATALOG}.gold.dim_match").select(
        "match_id", "map_name", "map_mode", "started_at", "ended_at", "match_result"
    )
    per_side = stats.groupBy("match_key", "match_id", "player_key").agg(
        F.count("*").alias("rounds_played"),
        F.sum("kills").alias("kills"),
        F.sum("headshot_kills").alias("headshot_kills"),
        F.sum("deaths").alias("deaths"),
        F.sum("damage_dealt").alias("damage_dealt"),
        F.sum("damage_taken").alias("damage_taken"),
        F.sum(F.when(F.col("player_survived"), 1).otherwise(0)).alias("rounds_survived"),
        F.sum(F.when(F.col("player_won"), 1).otherwise(0)).alias("rounds_won"),
        F.sum(F.when(F.col("player_team") == "CT", F.col("kills")).otherwise(0)).alias("kills_ct"),
        F.sum(F.when(F.col("player_team") == "T", F.col("kills")).otherwise(0)).alias("kills_t"),
        F.round(F.sum("distance_units")).alias("distance_units"),
    )
    return (
        per_side.join(matches, "match_id", "left")
        .withColumn(
            "kd_ratio",
            F.round(F.col("kills") / F.greatest(F.col("deaths"), F.lit(1)), 2),
        )
        .withColumn("adr", F.round(F.col("damage_dealt") / F.greatest(F.col("rounds_played"), F.lit(1)), 1))
        .withColumn(
            "headshot_pct",
            F.round(100 * F.col("headshot_kills") / F.greatest(F.col("kills"), F.lit(1)), 1),
        )
        .withColumn(
            "survival_pct",
            F.round(100 * F.col("rounds_survived") / F.greatest(F.col("rounds_played"), F.lit(1)), 1),
        )
    )
