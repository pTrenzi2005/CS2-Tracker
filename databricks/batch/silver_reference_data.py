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
# MAGIC Parsing lives in `reference_transforms.py` next to this notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import reference_transforms as rt

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


def write(df: DataFrame, table_name: str, comment: str) -> None:
    full_name = f"{CATALOG}.silver.{table_name}"
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_reference` - map metadata, radar transform, and image paths

# COMMAND ----------

manifest_maps = rt.manifest_maps(map_icons)
overview = rt.overview(map_icons)

map_reference = rt.expect_all_or_drop(
    rt.map_reference(manifest_maps, overview, map_assets, INGEST_RUN_ID),
    "map_reference",
    rt.MAP_REFERENCE_RULES,
)
write(
    map_reference,
    "map_reference",
    "Map metadata from cs2-map-icons: radar transform, zones, and images.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_level` - radar level, altitude range, and background image

# COMMAND ----------

map_level = rt.expect_all_or_drop(
    rt.map_level(manifest_maps, map_assets, INGEST_RUN_ID), "map_level", rt.MAP_LEVEL_RULES
)
write(map_level, "map_level", "Map radar levels with altitude range and background image.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_zone` - bombsite and spawn positions as radar-image fractions

# COMMAND ----------

map_zone = rt.expect_all_or_drop(
    rt.map_zone(overview, INGEST_RUN_ID), "map_zone", rt.MAP_ZONE_RULES
)
write(map_zone, "map_zone", "Bombsite and spawn zones as a fraction of the radar image.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.weapon_attributes` - weapon stats from the Raw Values sheet
# MAGIC
# MAGIC Names match GSI without the `weapon_` prefix. The Simple sheet stays in bronze.

# COMMAND ----------

weapon_attributes = rt.expect_all_or_drop(
    rt.weapon_attributes(weapon_raw), "weapon_attributes", rt.WEAPON_RULES
)
write(
    weapon_attributes,
    "weapon_attributes",
    "Weapon parameters from the Kaggle dataset Raw Values sheet.",
)
