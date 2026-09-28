# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (4/4) - Gold
# MAGIC
# MAGIC Overwritten gold dimensions for the latest silver snapshot.
# MAGIC Keys match the GSI star schema: `map_key = xxhash64(map_name)`, `weapon_key = xxhash64(gsi weapon name)`.
# MAGIC - `gold.dim_map_reference` — one row per map
# MAGIC - `gold.dim_map_level` — one row per map and radar level
# MAGIC - `gold.dim_map_zone` — one row per map and zone
# MAGIC - `gold.dim_weapon_spec` — one row per weapon
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")

# CS2 radars are 1024 x 1024. Y is flipped: pixels start at the top, game coords at map center.
# pixel_x = (pos_x - radar_pos_x) / radar_scale
# pixel_y = (radar_pos_y - pos_y) / radar_scale
RADAR_SIZE_PX = 1024

map_reference = spark.read.table(f"{CATALOG}.silver.map_reference")
map_level = spark.read.table(f"{CATALOG}.silver.map_level")
map_zone = spark.read.table(f"{CATALOG}.silver.map_zone")
weapon_attributes = spark.read.table(f"{CATALOG}.silver.weapon_attributes")

map_key = F.xxhash64(F.col("map_name")).alias("map_key")


def write(df: DataFrame, table_name: str, comment: str) -> None:
    full_name = f"{CATALOG}.gold.{table_name}"
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_map_reference` - map dimension with radar world bounds
# MAGIC

# COMMAND ----------

world_span = F.col("radar_scale") * F.lit(RADAR_SIZE_PX)

dim_map_reference = map_reference.select(
    map_key,
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

write(
    dim_map_reference,
    "dim_map_reference",
    "Map dimension: radar transform, images, and metadata.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_map_level` - radar level and altitude range
# MAGIC
# MAGIC Match a position with `pos_z >= altitude_min AND pos_z < altitude_max` so shared borders stay on one level.
# MAGIC

# COMMAND ----------

dim_map_level = map_level.select(
    map_key,
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

write(dim_map_level, "dim_map_level", "Map radar levels with altitude range and background image.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_map_zone` - bombsite and spawn positions in world coordinates
# MAGIC

# COMMAND ----------

zone_geometry = map_reference.select("map_name", "radar_pos_x", "radar_pos_y", "radar_scale")
zone_span = F.col("radar_scale") * F.lit(RADAR_SIZE_PX)

dim_map_zone = (
    map_zone.join(zone_geometry, "map_name", "inner")
    .select(
        map_key,
        "map_name",
        "zone_type",
        "frac_x",
        "frac_y",
        F.round(F.col("radar_pos_x") + F.col("frac_x") * zone_span, 1).alias("world_x"),
        F.round(F.col("radar_pos_y") - F.col("frac_y") * zone_span, 1).alias("world_y"),
        "ingest_run_id",
        "loaded_at",
    )
)

write(
    dim_map_zone,
    "dim_map_zone",
    "Bombsite and spawn zones in game world coordinates.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_weapon_spec` - weapon stats with derived fire rate and damage
# MAGIC

# COMMAND ----------

cycle_time = F.col("cycle_time_s")
# Ignores helmet, armor, and range falloff.
one_shot_headshot = F.col("damage") * F.col("headshot_multiplier") >= 100

dim_weapon_spec = weapon_attributes.select(
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

write(
    dim_weapon_spec,
    "dim_weapon_spec",
    "Weapon dimension; weapon_key matches the GSI pipeline dim_weapon.",
)
