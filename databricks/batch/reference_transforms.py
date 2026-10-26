"""Reference-data transformations: DataFrames in, DataFrames out.

The notebooks next to this file read bronze and write silver and gold. Notebook directories
are on sys.path, so they import this module directly. Tests run it on local Spark.
"""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def expect_all_or_drop(df: DataFrame, table_name: str, rules: dict[str, str]) -> DataFrame:
    """Drops rows that fail a rule and prints the counts. A null condition fails, as in Lakeflow."""
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


def blank_to_null(column: Column, cast_to: str) -> Column:
    return F.when(column == "", None).otherwise(column).cast(cast_to)


# Map manifest. `maps` and `verticalsections` use dynamic keys, so they are read as MAP and exploded.

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

RADAR = F.col("entry.radar_info")


def manifest_maps(map_icons: DataFrame) -> DataFrame:
    """One row per map from available.json: `map_name`, `entry`, `source_commit`."""
    return (
        map_icons.filter(F.col("artifact_type") == "map_manifest")
        .select("source_commit", F.from_json("raw_content", MANIFEST_SCHEMA).alias("manifest"))
        .select("source_commit", F.explode("manifest.maps").alias("map_name", "entry"))
    )


# Radar overview files are Valve KeyValues: "key" "value" pairs.


def key_value(key: str) -> Column:
    pattern = f'(?i)"{key}"\\s*"\\s*(-?[0-9]+(?:\\.[0-9]+)?)\\s*"'
    return blank_to_null(F.regexp_extract(F.col("raw_content"), pattern, 1), "double")


def overview(map_icons: DataFrame) -> DataFrame:
    """Insets, bombsites, and spawns from each radar overview file, as radar-image fractions."""
    return map_icons.filter(F.col("artifact_type") == "radar_info").select(
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


def map_family(map_name: Column) -> Column:
    prefix = F.split(map_name, "_").getItem(0)
    return (
        F.when(prefix == "de", "bomb_defusal")
        .when(prefix == "cs", "hostage")
        .when(prefix == "ar", "arms_race")
        .when(prefix == "dz", "danger_zone")
        .when(prefix == "gd", "guardian")
        .otherwise("other")
    )


MAP_REFERENCE_RULES = {
    "known_map": "map_name IS NOT NULL AND map_name <> ''",
    "complete_transform": (
        "NOT has_radar OR (radar_pos_x IS NOT NULL AND radar_pos_y IS NOT NULL "
        "AND radar_scale > 0)"
    ),
    "fractions_in_range": (
        "coalesce(bombsite_a_x, 0) BETWEEN 0 AND 1 AND coalesce(ct_spawn_x, 0) BETWEEN 0 AND 1"
    ),
}


def map_reference(
    maps: DataFrame, overview_df: DataFrame, map_assets: DataFrame, ingest_run_id: str
) -> DataFrame:
    icons = map_assets.filter(F.col("asset_type") == "icon").select(
        "map_name", F.col("volume_path").alias("icon_volume_path")
    )
    section_count = (
        F.when(RADAR["verticalsections"].isNull(), F.lit(0))
        .otherwise(F.size(RADAR["verticalsections"]))
    )
    return (
        maps.join(overview_df, "map_name", "left")
        .join(icons, "map_name", "left")
        .select(
            "map_name",
            F.coalesce(F.col("entry.display_name"), F.col("map_name")).alias("display_name"),
            map_family(F.col("map_name")).alias("map_family"),
            F.to_date("entry.first_seen").alias("first_seen_date"),
            F.col("entry.path").alias("icon_url"),
            "icon_volume_path",
            RADAR.isNotNull().alias("has_radar"),
            RADAR["pos_x"].alias("radar_pos_x"),
            RADAR["pos_y"].alias("radar_pos_y"),
            RADAR["scale"].alias("radar_scale"),
            F.coalesce(RADAR["rotate"], F.lit(0.0)).alias("radar_rotate"),
            F.coalesce(RADAR["zoom"], F.lit(1.0)).alias("radar_zoom"),
            "radar_material",
            F.when(section_count > 0, section_count).otherwise(F.lit(1)).alias("level_count"),
            F.coalesce(F.col("entry.radar_paths"), F.array().cast("array<string>")).alias(
                "radar_urls"
            ),
            F.coalesce(F.col("entry.thumb_paths"), F.array().cast("array<string>")).alias(
                "thumb_urls"
            ),
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
            F.lit(ingest_run_id).alias("ingest_run_id"),
            F.current_timestamp().alias("loaded_at"),
        )
    )


# Single-level maps get a synthetic `default` level covering the full altitude range.
ALTITUDE_LIMIT = 100000.0

# File names vary by source suffix (_psd, _tga, or none): de_nuke_lower_radar_psd.png.
IMAGE_SUFFIX = "(?i)(_(psd|tga|vtf|dds))?\\.png$"

MAP_LEVEL_RULES = {
    "known_level": "map_name IS NOT NULL AND level_name IS NOT NULL",
    "altitude_range": "altitude_min < altitude_max",
    "known_image": "radar_url IS NOT NULL",
}


def radar_base_name(url: Column) -> Column:
    return F.regexp_replace(F.element_at(F.split(url, "/"), -1), IMAGE_SUFFIX, "")


def map_level(maps: DataFrame, map_assets: DataFrame, ingest_run_id: str) -> DataFrame:
    with_radar = maps.filter(RADAR.isNotNull()).select(
        "map_name",
        F.coalesce(F.col("entry.radar_paths"), F.array().cast("array<string>")).alias("radar_urls"),
        RADAR["verticalsections"].alias("sections"),
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
    expected_name = F.concat(
        F.col("map_name"),
        F.when(F.col("level_name") == "default", F.lit("")).otherwise(
            F.concat(F.lit("_"), F.col("level_name"))
        ),
        F.lit("_radar"),
    )
    radar_assets = map_assets.filter(F.col("asset_type") == "radar").select(
        F.col("file_name").alias("radar_file_name"),
        F.col("volume_path").alias("radar_volume_path"),
    )
    return (
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
            F.lit(ingest_run_id).alias("ingest_run_id"),
            F.current_timestamp().alias("loaded_at"),
        )
    )


ZONE_KEYS = {
    "bombsite_a": ("bombsite_a_x", "bombsite_a_y"),
    "bombsite_b": ("bombsite_b_x", "bombsite_b_y"),
    "ct_spawn": ("ct_spawn_x", "ct_spawn_y"),
    "t_spawn": ("t_spawn_x", "t_spawn_y"),
}

MAP_ZONE_RULES = {
    "known_position": "frac_x IS NOT NULL AND frac_y IS NOT NULL",
    "fractions_in_range": "frac_x BETWEEN 0 AND 1 AND frac_y BETWEEN 0 AND 1",
}


def map_zone(overview_df: DataFrame, ingest_run_id: str) -> DataFrame:
    zone_frames = [
        overview_df.select(
            "map_name",
            F.lit(zone_type).alias("zone_type"),
            F.col(column_x).alias("frac_x"),
            F.col(column_y).alias("frac_y"),
        )
        for zone_type, (column_x, column_y) in ZONE_KEYS.items()
    ]
    return (
        reduce(DataFrame.unionByName, zone_frames)
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("loaded_at", F.current_timestamp())
    )


# Weapons: the Raw Values sheet. A header line starts each weapon category.

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

WEAPON_RULES = {
    "known_weapon": "weapon_name IS NOT NULL AND weapon_name <> ''",
    "positive_damage": "damage > 0",
    "positive_cycle_time": "cycle_time_s > 0",
    "known_price": "price_usd IS NOT NULL AND price_usd >= 0",
    "known_type": "weapon_type IS NOT NULL",
}


def money(column: str) -> Column:
    return F.regexp_replace(F.col(column), "[^0-9]", "").cast("int")


def weapon_attributes(weapon_raw: DataFrame) -> DataFrame:
    raw_values = weapon_raw.filter(F.col("file_name") == "weapons_raw_values.csv")
    is_header = F.col("raw_line").contains("WeaporArmorRatio")
    is_blank = F.regexp_replace(F.col("raw_line"), ",", "").rlike("^\\s*$")
    lines_in_order = Window.partitionBy("file_name").orderBy("line_number")

    parsed = (
        raw_values.withColumn(
            "weapon_category",
            F.last(
                F.when(is_header, F.split(F.col("raw_line"), ",").getItem(0)), ignorenulls=True
            ).over(lines_in_order),
        )
        .filter(~is_header & ~is_blank)
        .withColumn("parsed", F.from_csv(F.col("raw_line"), WEAPON_CSV_SCHEMA))
        .select("line_number", "weapon_category", "ingest_run_id", "parsed.*")
    )
    return parsed.select(
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


# Gold. CS2 radars are 1024 x 1024. Y is flipped: pixels start at the top, game coords at map center.
# pixel_x = (pos_x - radar_pos_x) / radar_scale
# pixel_y = (radar_pos_y - pos_y) / radar_scale
RADAR_SIZE_PX = 1024


def map_key() -> Column:
    return F.xxhash64(F.col("map_name")).alias("map_key")


def dim_map_reference(map_reference_df: DataFrame) -> DataFrame:
    world_span = F.col("radar_scale") * F.lit(RADAR_SIZE_PX)
    return map_reference_df.select(
        map_key(),
        "map_name",
        "display_name",
        "map_family",
        "first_seen_date",
        "has_radar",
        "level_count",
        "radar_pos_x",
        "radar_pos_y",
        "radar_scale",
        "radar_rotate",
        "radar_zoom",
        F.lit(RADAR_SIZE_PX).alias("radar_size_px"),
        F.col("radar_pos_x").alias("world_min_x"),
        (F.col("radar_pos_x") + world_span).alias("world_max_x"),
        (F.col("radar_pos_y") - world_span).alias("world_min_y"),
        F.col("radar_pos_y").alias("world_max_y"),
        "inset_left",
        "inset_top",
        "inset_right",
        "inset_bottom",
        "icon_url",
        "icon_volume_path",
        "radar_urls",
        "thumb_urls",
        "radar_material",
        "source_hash",
        "source_commit",
        "ingest_run_id",
        "loaded_at",
    )


def dim_map_level(map_level_df: DataFrame) -> DataFrame:
    """Match a position with `pos_z >= altitude_min AND pos_z < altitude_max`, so shared borders stay on one level."""
    return map_level_df.select(
        map_key(),
        "map_name",
        "level_name",
        "is_default_level",
        "altitude_min",
        "altitude_max",
        "radar_url",
        "radar_volume_path",
        "ingest_run_id",
        "loaded_at",
    )


def dim_map_zone(map_zone_df: DataFrame, map_reference_df: DataFrame) -> DataFrame:
    zone_geometry = map_reference_df.select("map_name", "radar_pos_x", "radar_pos_y", "radar_scale")
    zone_span = F.col("radar_scale") * F.lit(RADAR_SIZE_PX)
    return map_zone_df.join(zone_geometry, "map_name", "inner").select(
        map_key(),
        "map_name",
        "zone_type",
        "frac_x",
        "frac_y",
        F.round(F.col("radar_pos_x") + F.col("frac_x") * zone_span, 1).alias("world_x"),
        F.round(F.col("radar_pos_y") - F.col("frac_y") * zone_span, 1).alias("world_y"),
        "ingest_run_id",
        "loaded_at",
    )


def dim_weapon_spec(weapon_attributes_df: DataFrame) -> DataFrame:
    cycle_time = F.col("cycle_time_s")
    # Ignores helmet, armor, and range falloff.
    one_shot_headshot = F.col("damage") * F.col("headshot_multiplier") >= 100
    return weapon_attributes_df.select(
        F.xxhash64(F.col("gsi_weapon_name")).alias("weapon_key"),
        "gsi_weapon_name",
        F.col("weapon_name").alias("weapon_short_name"),
        "weapon_category",
        "weapon_type",
        "price_usd",
        "kill_award_usd",
        "damage",
        "bullets",
        "clip_size",
        "armor_penetration_ratio",
        "headshot_multiplier",
        "penetration_power",
        "range_modifier",
        "range_units",
        "cycle_time_s",
        F.round(F.lit(60.0) / cycle_time, 1).alias("fire_rate_rpm"),
        F.round(F.col("damage") * F.col("bullets") / cycle_time, 1).alias("damage_per_second"),
        one_shot_headshot.alias("kills_with_one_headshot"),
        "is_full_auto",
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
        "ingest_run_id",
        "loaded_at",
    )
