"""Silver: zdarzenia wyliczone z roznic miedzy kolejnymi tickami.

GSI nie wysyla zdarzen typu "zabojstwo" ani "obrazenia" - wysyla stan. Zabicie
poznajemy po wzroscie licznika ``round_kills``, smierc po zejsciu zycia do
zera, obrazenia po zmianie ``round_totaldmg``.

Te tabele sa materialized view, a nie streaming table, i jest to swiadome:
``lag()`` wymaga uporzadkowania calej partycji, czego nie da sie policzyc
przyrostowo na strumieniu dopisywanym. Przy skali jednego meczu (kilkanascie
tysiecy tickow) pelne przeliczenie jest tanie.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("cs2.catalog")
TICKS_TABLE = f"{CATALOG}.silver.player_ticks"
ROUND_EVENTS_TABLE = f"{CATALOG}.silver.round_events"

BY_ROUND = Window.partitionBy("match_id", "round_number").orderBy("seq")
BY_MATCH = Window.partitionBy("match_id").orderBy("seq")


def ticks_with_deltas():
    """Ticki wzbogacone o wartosci z poprzedniej probki."""
    return (
        spark.read.table(TICKS_TABLE)
        .withColumn("prev_round_kills", F.lag("round_kills").over(BY_ROUND))
        .withColumn("prev_round_killhs", F.lag("round_killhs").over(BY_ROUND))
        .withColumn("prev_round_totaldmg", F.lag("round_totaldmg").over(BY_ROUND))
        .withColumn("prev_health", F.lag("health").over(BY_ROUND))
        .withColumn("prev_pos_x", F.lag("pos_x").over(BY_ROUND))
        .withColumn("prev_pos_y", F.lag("pos_y").over(BY_ROUND))
        .withColumn("prev_pos_z", F.lag("pos_z").over(BY_ROUND))
        # Licznik zabojstw z match_stats jest kumulatywny, wiec porownujemy go
        # w obrebie calego meczu, a nie pojedynczej rundy.
        .withColumn("prev_match_kills", F.lag("match_kills").over(BY_MATCH))
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.kill_events",
    comment="Ticki, w ktorych licznik zabojstw naszego gracza wzrosl.",
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
            # Przy probkowaniu co ~0.15 s dwa zabojstwa moga wpasc w jedna
            # probke, dlatego zdarzenie niesie licznik zamiast udawac, ze
            # zawsze opisuje dokladnie jedno zabojstwo.
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
    comment="Smierc gracza w rundzie, zlozona z tickow i ze zdarzen konca rundy.",
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
    # Gdy zginiemy dokladnie na koniec rundy, ostatni tick moze juz nie
    # dotrzec - stan koncowy zna wtedy tylko zdarzenie round_end.
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
        # W rundzie ginie sie najwyzej raz; tick opisuje smierc dokladniej niz
        # podsumowanie rundy, wiec ma pierwszenstwo.
        .withColumn("_rank", F.row_number().over(dedup))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


@dp.materialized_view(
    name=f"{CATALOG}.silver.damage_events",
    comment="Obrazenia zadane i otrzymane, wyliczone z przyrostow stanu.",
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
    # Obrazenia otrzymane liczymy ze spadku zycia, wiec nie obejmuja tej czesci,
    # ktora pochlonal pancerz.
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
    comment="Podsumowanie tickow w obrebie rundy: czas, dystans, ekonomia.",
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
