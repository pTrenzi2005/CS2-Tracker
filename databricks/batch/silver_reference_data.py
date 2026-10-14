# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (3/4) - Silver
# MAGIC
# MAGIC Overwritten silver tables for the latest bronze snapshot (`ingest_run_id`):
# MAGIC - `silver.map_reference` — one row per map
# MAGIC - `silver.map_level` — one row per map and radar level
# MAGIC - `silver.map_zone` — one row per map and zone
# MAGIC - `silver.weapon_attributes` — one row per weapon
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = dbutils.widgets.get("catalog")
MAP_ICONS_RAW = f"{CATALOG}.bronze.map_icons_raw"
MAP_ASSETS_RAW = f"{CATALOG}.bronze.map_assets_raw"
WEAPON_RAW = f"{CATALOG}.bronze.weapon_spreadsheet_raw"

INGEST_RUN_ID = dbutils.jobs.taskValues.get(taskKey="download", key="ingest_run_id", debugValue="")
if not INGEST_RUN_ID:
    INGEST_RUN_ID = spark.read.table(MAP_ICONS_RAW).agg(F.max("ingest_run_id")).collect()[0][0]

map_icons = spark.read.table(MAP_ICONS_RAW).filter(F.col("ingest_run_id") == INGEST_RUN_ID)
map_assets = spark.read.table(MAP_ASSETS_RAW).filter(F.col("ingest_run_id") == INGEST_RUN_ID)
weapon_raw = spark.read.table(WEAPON_RAW).filter(F.col("ingest_run_id") == INGEST_RUN_ID)

#expectation
def expect_all_or_drop(df: DataFrame, table_name: str, rules: dict[str, str]) -> DataFrame:
    violations = (
        df.agg(
            *[F.sum(F.when(F.expr(rule), 0).otherwise(1)).alias(name) for name, rule in rules.items()]
        )
        .collect()[0]
        .asDict()
    )
    failed = {name: count for name, count in violations.items() if count}
    if failed:
        print(f"{table_name}: dropped rows {failed}")
    condition = reduce(lambda left, right: left & right, [F.expr(rule) for rule in rules.values()])
    return df.filter(condition)


def write(df: DataFrame, table_name: str, comment: str) -> None:
    full_name = f"{CATALOG}.silver.{table_name}"
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")


def blank_to_null(column, cast_to: str):
    return F.when(column == "", None).otherwise(column).cast(cast_to)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Map manifest
# MAGIC
# MAGIC `maps` and `verticalsections` use dynamic keys, so they are read as `MAP` and exploded.
# MAGIC

# COMMAND ----------

VERTICAL_SECTION = "STRUCT<AltitudeMax: DOUBLE, AltitudeMin: DOUBLE>"
RADAR_INFO = (
    "STRUCT<path: STRING, pos_x: DOUBLE, pos_y: DOUBLE, scale: DOUBLE, rotate: DOUBLE, "
    f"zoom: DOUBLE, verticalsections: MAP<STRING, {VERTICAL_SECTION}>>"
)
MAP_ENTRY = (
    "STRUCT<origin: STRING, hash: STRING, display_name: STRING, first_seen: STRING, "
    f"path: STRING, radar_paths: ARRAY<STRING>, radar_info: {RADAR_INFO}, "
    "thumb_paths: ARRAY<STRING>>"
)
MANIFEST_SCHEMA = f"STRUCT<count: INT, maps: MAP<STRING, {MAP_ENTRY}>>"

manifest_maps = (
    map_icons.filter(F.col("artifact_type") == "map_manifest")
    .select("source_commit", F.from_json("raw_content", MANIFEST_SCHEMA).alias("manifest"))
    .select("source_commit", F.explode("manifest.maps").alias("map_name", "entry"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Radar overview files
# MAGIC

# COMMAND ----------

radar_text = map_icons.filter(F.col("artifact_type") == "radar_info").select(
    "map_name", "raw_content"
)


def key_value(key: str):
    pattern = f'(?i)"{key}"\\s*"\\s*(-?[0-9]+(?:\\.[0-9]+)?)\\s*"'
    return blank_to_null(F.regexp_extract(F.col("raw_content"), pattern, 1), "double")


overview = radar_text.select(
    "map_name",
    blank_to_null(
        F.regexp_extract(F.col("raw_content"), '(?i)"material"\\s*"([^"]+)"', 1), "string"
    ).alias("radar_material"),
    key_value("inset_left").alias("inset_left"),
    key_value("inset_top").alias("inset_top"),
    key_value("inset_right").alias("inset_right"),
    key_value("inset_bottom").alias("inset_bottom"),
    key_value("bombA_x").alias("bombsite_a_x"),
    key_value("bombA_y").alias("bombsite_a_y"),
    key_value("bombB_x").alias("bombsite_b_x"),
    key_value("bombB_y").alias("bombsite_b_y"),
    key_value("CTSpawn_x").alias("ct_spawn_x"),
    key_value("CTSpawn_y").alias("ct_spawn_y"),
    key_value("TSpawn_x").alias("t_spawn_x"),
    key_value("TSpawn_y").alias("t_spawn_y"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_reference` - map metadata, radar transform, and image paths
# MAGIC

# COMMAND ----------

icons = map_assets.filter(F.col("asset_type") == "icon").select(
    "map_name", F.col("volume_path").alias("icon_volume_path")
)

prefix = F.split(F.col("map_name"), "_").getItem(0)
radar = F.col("entry.radar_info")
section_count = (
    F.when(radar["verticalsections"].isNull(), F.lit(0))
    .otherwise(F.size(radar["verticalsections"]))
)

map_reference = (
    manifest_maps.join(overview, "map_name", "left")
    .join(icons, "map_name", "left")
    .select(
        "map_name",
        F.coalesce(F.col("entry.display_name"), F.col("map_name")).alias("display_name"),
        F.when(prefix == "de", "bomb_defusal")
        .when(prefix == "cs", "hostage")
        .when(prefix == "ar", "arms_race")
        .when(prefix == "dz", "danger_zone")
        .when(prefix == "gd", "guardian")
        .otherwise("other")
        .alias("map_family"),
        F.to_date("entry.first_seen").alias("first_seen_date"),
        F.col("entry.path").alias("icon_url"),
        "icon_volume_path",
        radar.isNotNull().alias("has_radar"),
        radar["pos_x"].alias("radar_pos_x"),
        radar["pos_y"].alias("radar_pos_y"),
        radar["scale"].alias("radar_scale"),
        F.coalesce(radar["rotate"], F.lit(0.0)).alias("radar_rotate"),
        F.coalesce(radar["zoom"], F.lit(1.0)).alias("radar_zoom"),
        "radar_material",
        F.when(section_count > 0, section_count).otherwise(F.lit(1)).alias("level_count"),
        F.coalesce(F.col("entry.radar_paths"), F.array().cast("array<string>")).alias("radar_urls"),
        F.coalesce(F.col("entry.thumb_paths"), F.array().cast("array<string>")).alias("thumb_urls"),
        "inset_left",
        "inset_top",
        "inset_right",
        "inset_bottom",
        "bombsite_a_x",
        "bombsite_a_y",
        "bombsite_b_x",
        "bombsite_b_y",
        "ct_spawn_x",
        "ct_spawn_y",
        "t_spawn_x",
        "t_spawn_y",
        F.col("entry.hash").alias("source_hash"),
        F.col("entry.origin").alias("source_origin"),
        "source_commit",
        F.lit(INGEST_RUN_ID).alias("ingest_run_id"),
        F.current_timestamp().alias("loaded_at"),
    )
)

map_reference = expect_all_or_drop(
    map_reference,
    "map_reference",
    {
        "known_map": "map_name IS NOT NULL AND map_name <> ''",
        "complete_transform": (
            "NOT has_radar OR (radar_pos_x IS NOT NULL AND radar_pos_y IS NOT NULL "
            "AND radar_scale > 0)"
        ),
        "fractions_in_range": (
            "coalesce(bombsite_a_x, 0) BETWEEN 0 AND 1 AND coalesce(ct_spawn_x, 0) BETWEEN 0 AND 1"
        ),
    },
)
write(
    map_reference,
    "map_reference",
    "Map metadata from cs2-map-icons: radar transform, zones, and images.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_level` - radar level, altitude range, and background image
# MAGIC
# MAGIC Single-level maps get a synthetic `default` level covering the full altitude range.
# MAGIC

# COMMAND ----------

ALTITUDE_LIMIT = 100000.0

with_radar = manifest_maps.filter(radar.isNotNull()).select(
    "map_name",
    F.coalesce(F.col("entry.radar_paths"), F.array().cast("array<string>")).alias("radar_urls"),
    radar["verticalsections"].alias("sections"),
)

has_sections = F.col("sections").isNotNull() & (F.size(F.col("sections")) > 0)

multi_level = (
    with_radar.filter(has_sections)
    .select("map_name", "radar_urls", F.explode("sections").alias("level_name", "section"))
    .select(
        "map_name",
        "radar_urls",
        "level_name",
        F.col("section.AltitudeMin").alias("altitude_min"),
        F.col("section.AltitudeMax").alias("altitude_max"),
    )
)

single_level = with_radar.filter(~has_sections).select(
    "map_name",
    "radar_urls",
    F.lit("default").alias("level_name"),
    F.lit(-ALTITUDE_LIMIT).alias("altitude_min"),
    F.lit(ALTITUDE_LIMIT).alias("altitude_max"),
)

# File names vary by source suffix (_psd, _tga, or none): de_nuke_lower_radar_psd.png.
IMAGE_SUFFIX = "(?i)(_(psd|tga|vtf|dds))?\\.png$"

expected_name = F.concat(
    F.col("map_name"),
    F.when(F.col("level_name") == "default", F.lit("")).otherwise(
        F.concat(F.lit("_"), F.col("level_name"))
    ),
    F.lit("_radar"),
)


def radar_base_name(url):
    return F.regexp_replace(F.element_at(F.split(url, "/"), -1), IMAGE_SUFFIX, "")


radar_assets = map_assets.filter(F.col("asset_type") == "radar").select(
    F.col("file_name").alias("radar_file_name"),
    F.col("volume_path").alias("radar_volume_path"),
)

map_level = (
    multi_level.unionByName(single_level)
    .withColumn(
        "radar_url",
        F.element_at(
            F.filter(F.col("radar_urls"), lambda url: radar_base_name(url) == expected_name), 1
        ),
    )
    .withColumn("radar_file_name", F.element_at(F.split(F.col("radar_url"), "/"), -1))
    .join(radar_assets, "radar_file_name", "left")
    .select(
        "map_name",
        "level_name",
        (F.col("level_name") == "default").alias("is_default_level"),
        "altitude_min",
        "altitude_max",
        "radar_url",
        "radar_volume_path",
        F.lit(INGEST_RUN_ID).alias("ingest_run_id"),
        F.current_timestamp().alias("loaded_at"),
    )
)

map_level = expect_all_or_drop(
    map_level,
    "map_level",
    {
        "known_level": "map_name IS NOT NULL AND level_name IS NOT NULL",
        "altitude_range": "altitude_min < altitude_max",
        "known_image": "radar_url IS NOT NULL",
    },
)
write(map_level, "map_level", "Map radar levels with altitude range and background image.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_zone` - bombsite and spawn positions as radar-image fractions
# MAGIC

# COMMAND ----------

ZONE_KEYS = {
    "bombsite_a": ("bombsite_a_x", "bombsite_a_y"),
    "bombsite_b": ("bombsite_b_x", "bombsite_b_y"),
    "ct_spawn": ("ct_spawn_x", "ct_spawn_y"),
    "t_spawn": ("t_spawn_x", "t_spawn_y"),
}

zone_frames = [
    overview.select(
        "map_name",
        F.lit(zone_type).alias("zone_type"),
        F.col(column_x).alias("frac_x"),
        F.col(column_y).alias("frac_y"),
    )
    for zone_type, (column_x, column_y) in ZONE_KEYS.items()
]

map_zone = (
    reduce(DataFrame.unionByName, zone_frames)
    .withColumn("ingest_run_id", F.lit(INGEST_RUN_ID))
    .withColumn("loaded_at", F.current_timestamp())
)

map_zone = expect_all_or_drop(
    map_zone,
    "map_zone",
    {
        "known_position": "frac_x IS NOT NULL AND frac_y IS NOT NULL",
        "fractions_in_range": "frac_x BETWEEN 0 AND 1 AND frac_y BETWEEN 0 AND 1",
    },
)
write(map_zone, "map_zone", "Bombsite and spawn zones as a fraction of the radar image.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.weapon_attributes` - weapon stats from the Raw Values sheet
# MAGIC
# MAGIC Names match GSI without the `weapon_` prefix. The Simple sheet stays in bronze.
# MAGIC

# COMMAND ----------

WEAPON_CSV_SCHEMA = ", ".join(
    [
        "weapon_name STRING",
        "armor_ratio DOUBLE",
        "damage INT",
        "range_modifier DOUBLE",
        "headshot_multiplier DOUBLE",
        "cycle_time_s DOUBLE",
        "penetration_power DOUBLE",
        "kill_award_raw STRING",
        "max_player_speed DOUBLE",
        "clip_size INT",
        "price_raw STRING",
        "range_units DOUBLE",
        "weapon_type STRING",
        "full_auto INT",
        "bullets INT",
        "tracer_frequency INT",
        "flinch_velocity_large DOUBLE",
        "flinch_velocity_small DOUBLE",
        "spread DOUBLE",
        "inaccuracy_crouch DOUBLE",
        "inaccuracy_stand DOUBLE",
        "inaccuracy_fire DOUBLE",
        "inaccuracy_move DOUBLE",
        "inaccuracy_jump DOUBLE",
        "inaccuracy_jump_initial DOUBLE",
        "inaccuracy_jump_apex DOUBLE",
        "inaccuracy_land DOUBLE",
        "inaccuracy_ladder DOUBLE",
        "recovery_time_crouch DOUBLE",
        "recovery_time_crouch_final DOUBLE",
        "recovery_time_stand DOUBLE",
        "recovery_time_stand_final DOUBLE",
        "recoil_angle_variance DOUBLE",
        "recoil_magnitude DOUBLE",
        "recoil_magnitude_variance DOUBLE",
        "spread_alt DOUBLE",
        "inaccuracy_crouch_alt DOUBLE",
        "inaccuracy_stand_alt DOUBLE",
        "inaccuracy_fire_alt DOUBLE",
        "inaccuracy_move_alt DOUBLE",
        "inaccuracy_jump_alt DOUBLE",
        "inaccuracy_land_alt DOUBLE",
        "inaccuracy_ladder_alt DOUBLE",
        "recoil_angle_variance_alt DOUBLE",
        "recoil_magnitude_alt DOUBLE",
        "recoil_magnitude_variance_alt DOUBLE",
        "max_player_speed_alt DOUBLE",
        "tracer_frequency_alt INT",
        "zoom_fov_1 DOUBLE",
        "zoom_fov_2 DOUBLE",
        "cycle_time_alt_s DOUBLE",
        "cycle_time_burst_s DOUBLE",
        "time_between_burst_shots_s DOUBLE",
    ]
)


def money(column: str):
    return F.regexp_replace(F.col(column), "[^0-9]", "").cast("int")


raw_values = weapon_raw.filter(F.col("file_name") == "weapons_raw_values.csv")

is_header = F.col("raw_line").contains("WeaporArmorRatio")
is_blank = F.regexp_replace(F.col("raw_line"), ",", "").rlike("^\\s*$")
lines_in_order = Window.partitionBy("file_name").orderBy("line_number")

parsed = (
    raw_values.withColumn(
        "weapon_category",
        F.last(F.when(is_header, F.split(F.col("raw_line"), ",").getItem(0)), ignorenulls=True).over(
            lines_in_order
        ),
    )
    .filter(~is_header & ~is_blank)
    .withColumn("parsed", F.from_csv(F.col("raw_line"), WEAPON_CSV_SCHEMA))
    .select("line_number", "weapon_category", "ingest_run_id", "parsed.*")
)

weapon_attributes = parsed.select(
    "weapon_name",
    F.concat(F.lit("weapon_"), F.col("weapon_name")).alias("gsi_weapon_name"),
    "weapon_category",
    "weapon_type",
    money("price_raw").alias("price_usd"),
    money("kill_award_raw").alias("kill_award_usd"),
    "damage",
    "bullets",
    "clip_size",
    F.col("armor_ratio").alias("armor_penetration_ratio"),
    "range_modifier",
    "range_units",
    "headshot_multiplier",
    "penetration_power",
    "cycle_time_s",
    (F.col("full_auto") == 1).alias("is_full_auto"),
    "max_player_speed",
    "spread",
    "inaccuracy_stand",
    "inaccuracy_crouch",
    "inaccuracy_move",
    "inaccuracy_jump",
    "inaccuracy_fire",
    "recovery_time_stand",
    "recovery_time_crouch",
    "recoil_magnitude",
    "recoil_angle_variance",
    "zoom_fov_1",
    "zoom_fov_2",
    "cycle_time_alt_s",
    "cycle_time_burst_s",
    "time_between_burst_shots_s",
    "line_number",
    "ingest_run_id",
    F.current_timestamp().alias("loaded_at"),
)

weapon_attributes = expect_all_or_drop(
    weapon_attributes,
    "weapon_attributes",
    {
        "known_weapon": "weapon_name IS NOT NULL AND weapon_name <> ''",
        "positive_damage": "damage > 0",
        "positive_cycle_time": "cycle_time_s > 0",
        "known_price": "price_usd IS NOT NULL AND price_usd >= 0",
        "known_type": "weapon_type IS NOT NULL",
    },
)
write(
    weapon_attributes,
    "weapon_attributes",
    "Weapon parameters from the Kaggle dataset Raw Values sheet.",
)
