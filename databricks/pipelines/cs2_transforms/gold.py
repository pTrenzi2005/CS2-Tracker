"""Gold: star-schema dimensions and facts. Keys are hashes of natural keys, so full recomputes stay stable."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def weapon_class(weapon_type: Column) -> Column:
    return (
        F.when(weapon_type.isin("Knife", "Melee", "Fists"), "melee")
        .when(weapon_type == "Pistol", "sidearm")
        .when(
            weapon_type.isin("Rifle", "SniperRifle", "Submachine Gun", "Shotgun", "Machine Gun"),
            "primary",
        )
        .when(weapon_type == "Grenade", "utility")
        .when(weapon_type == "C4", "objective")
        .otherwise("other")
    )


def round_type(equip_value: Column) -> Column:
    """Economy class from the peak equip value. round_start arrives in freeze time, before buys."""
    return (
        F.when(equip_value.isNull(), "unknown")
        .when(equip_value < 2000, "eco")
        .when(equip_value < 3500, "force")
        .otherwise("full")
    )


def dim_player(ticks: DataFrame) -> DataFrame:
    return (
        ticks.groupBy("steamid")
        .agg(
            F.expr("max_by(player_name, seq)").alias("player_name"),
            F.min("client_ts").alias("first_seen_ts"),
            F.max("client_ts").alias("last_seen_ts"),
            F.countDistinct("match_id").alias("matches_played"),
        )
        .select(
            F.sha2(F.col("steamid"), 256).alias("player_key"),
            # Raw SteamID64 stays in silver.
            F.concat(F.substring(F.col("steamid"), 1, 8), F.lit("*********")).alias(
                "steamid_masked"
            ),
            "player_name",
            "first_seen_ts",
            "last_seen_ts",
            "matches_played",
        )
    )


def dim_map(ticks: DataFrame) -> DataFrame:
    return (
        ticks.groupBy("map_name")
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


def dim_weapon(ticks: DataFrame) -> DataFrame:
    return (
        ticks.select(F.explode("weapons").alias("weapon"))
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
            weapon_class(F.col("weapon_type")).alias("weapon_class"),
            (F.col("weapon_type") == "Grenade").alias("is_grenade"),
        )
    )


def dim_round(rounds: DataFrame, round_state: DataFrame) -> DataFrame:
    starts = (
        rounds.filter(F.col("event_type") == "round_start")
        .groupBy("match_id", "round_number")
        .agg(
            F.min("client_ts").alias("round_start_ts"),
            F.expr("min_by(player_team, seq)").alias("team_at_start"),
            F.expr("min_by(money, seq)").alias("money_start"),
            F.expr("min_by(map_name, seq)").alias("map_name_start"),
            F.expr("min_by(map_mode, seq)").alias("map_mode"),
            F.expr("min_by(steamid, seq)").alias("steamid_start"),
        )
    )
    ends = (
        rounds.filter(F.col("event_type") == "round_end")
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
    state = round_state.select(
        "match_id",
        "round_number",
        F.col("player_team").alias("team_from_ticks"),
        F.col("money_start").alias("money_first_tick"),
        F.col("equip_value_max").alias("equip_value_max"),
    )

    player_team = F.coalesce("team_at_start", "team_at_end", "team_from_ticks")
    equip = F.col("equip_value_max")
    by_match = Window.partitionBy("match_id").orderBy("round_number")
    return (
        starts.join(ends, ["match_id", "round_number"], "full_outer")
        .join(state, ["match_id", "round_number"], "left")
        .withColumn("player_team", player_team)
        .withColumn("map_name", F.coalesce("map_name_start", "map_name_end"))
        # Pistol round is round 0 or the first round after a side swap. Works for MR12 and wingman.
        .withColumn(
            "is_pistol_round",
            F.coalesce(
                (F.col("round_number") == 0)
                | (F.col("player_team") != F.lag("player_team").over(by_match)),
                F.lit(False),
            ),
        )
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
            F.coalesce("money_start", "money_first_tick").alias("money_start"),
            equip.alias("equip_value"),
            round_type(equip).alias("round_type"),
            "is_pistol_round",
            "score_ct_after",
            "score_t_after",
        )
    )


def dim_match(dim_round_df: DataFrame, matches: DataFrame) -> DataFrame:
    per_match = dim_round_df.groupBy("match_id").agg(
        F.count("*").alias("rounds_played"),
        F.sum(F.when(F.col("player_won"), 1).otherwise(0)).alias("rounds_won"),
        F.sum(F.when(F.col("player_survived"), 1).otherwise(0)).alias("rounds_survived"),
        F.min("round_start_ts").alias("first_round_ts"),
        F.max("round_end_ts").alias("last_round_ts"),
        # The scoreboard flips after a side swap and the last snapshot excludes that round.
        F.sum(F.when(F.col("win_team") == "CT", 1).otherwise(0)).alias("rounds_won_ct_side"),
        F.sum(F.when(F.col("win_team") == "T", 1).otherwise(0)).alias("rounds_won_t_side"),
        F.expr("min_by(map_name, round_number)").alias("map_name"),
        F.expr("min_by(map_mode, round_number)").alias("map_mode"),
        F.expr("min_by(player_key, round_number)").alias("player_key"),
    )
    boundaries = matches.groupBy("match_id").agg(
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
            "rounds_won_ct_side",
            "rounds_won_t_side",
            F.when(F.col("rounds_won") > rounds_lost, "win")
            .when(F.col("rounds_won") < rounds_lost, "loss")
            .otherwise("draw")
            .alias("match_result"),
        )
    )


def with_keys(df: DataFrame, weapon_column: str | None = None) -> DataFrame:
    out = (
        df.withColumn("match_key", F.xxhash64("match_id"))
        .withColumn("round_key", F.xxhash64(F.col("match_id"), F.col("round_number")))
        .withColumn("player_key", F.sha2(F.col("steamid"), 256))
        .withColumn("map_key", F.xxhash64("map_name"))
    )
    if weapon_column is not None:
        out = out.withColumn("weapon_key", F.xxhash64(F.col(weapon_column)))
    return out


def with_time_in_round(df: DataFrame, dim_round_df: DataFrame) -> DataFrame:
    """Seconds since round start."""
    rounds = dim_round_df.select("match_id", "round_number", "round_start_ts")
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


def fact_player_positions(ticks: DataFrame, dim_round_df: DataFrame) -> DataFrame:
    return with_time_in_round(with_keys(ticks, "active_weapon"), dim_round_df).select(
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


def fact_kills(kills: DataFrame, dim_round_df: DataFrame) -> DataFrame:
    return with_time_in_round(with_keys(kills, "weapon_name"), dim_round_df).select(
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


def fact_deaths(deaths: DataFrame, dim_round_df: DataFrame) -> DataFrame:
    return with_time_in_round(with_keys(deaths, "weapon_held"), dim_round_df).select(
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


def fact_damage(damage: DataFrame, dim_round_df: DataFrame) -> DataFrame:
    return with_time_in_round(with_keys(damage, "weapon_name"), dim_round_df).select(
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


def fact_round_stats(
    dim_round_df: DataFrame,
    round_state: DataFrame,
    kills: DataFrame,
    deaths: DataFrame,
    damage: DataFrame,
    rounds: DataFrame,
) -> DataFrame:
    state = round_state.select(
        "match_id",
        "round_number",
        "round_kills",
        "round_killhs",
        "round_damage",
        "distance_units",
        "tick_count",
        "min_health",
    )
    kills_per_round = kills.groupBy("match_id", "round_number").agg(
        F.sum("kill_count").alias("kills"),
        F.sum("headshot_count").alias("headshot_kills"),
    )
    deaths_per_round = deaths.groupBy("match_id", "round_number").agg(
        F.min("client_ts").alias("death_ts")
    )
    damage_per_round = damage.groupBy("match_id", "round_number").agg(
        F.sum(F.when(F.col("damage_type") == "dealt", F.col("damage")).otherwise(0)).alias(
            "damage_dealt"
        ),
        F.sum(F.when(F.col("damage_type") == "taken", F.col("damage")).otherwise(0)).alias(
            "damage_taken"
        ),
    )
    # Ticks stop at death. round_end still has the final counters.
    final_state = (
        rounds.filter(F.col("event_type") == "round_end")
        .groupBy("match_id", "round_number")
        .agg(
            F.expr("max_by(round_kills, seq)").alias("final_kills"),
            F.expr("max_by(round_killhs, seq)").alias("final_headshots"),
            F.expr("max_by(round_totaldmg, seq)").alias("final_damage"),
        )
    )

    joined = (
        dim_round_df.join(state, ["match_id", "round_number"], "left")
        .join(kills_per_round, ["match_id", "round_number"], "left")
        .join(deaths_per_round, ["match_id", "round_number"], "left")
        .join(damage_per_round, ["match_id", "round_number"], "left")
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
        # Kills that have a tick, and therefore a map position.
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
        F.col("equip_value").alias("equip_value"),
        F.coalesce(F.col("tick_count"), F.lit(0)).alias("tick_count"),
    )


def agg_match_summary(round_stats: DataFrame, dim_match_df: DataFrame) -> DataFrame:
    matches = dim_match_df.select(
        "match_id", "map_name", "map_mode", "started_at", "ended_at", "match_result"
    )
    per_side = round_stats.groupBy("match_key", "match_id", "player_key").agg(
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
        .withColumn(
            "adr", F.round(F.col("damage_dealt") / F.greatest(F.col("rounds_played"), F.lit(1)), 1)
        )
        .withColumn(
            "headshot_pct",
            F.round(100 * F.col("headshot_kills") / F.greatest(F.col("kills"), F.lit(1)), 1),
        )
        .withColumn(
            "survival_pct",
            F.round(100 * F.col("rounds_survived") / F.greatest(F.col("rounds_played"), F.lit(1)), 1),
        )
    )
