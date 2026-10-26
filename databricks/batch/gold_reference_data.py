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
# MAGIC Derivations live in `reference_transforms.py` next to this notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

from pyspark.sql import DataFrame

import reference_transforms as rt

CATALOG = dbutils.widgets.get("catalog")

map_reference = spark.read.table(f"{CATALOG}.silver.map_reference")
map_level = spark.read.table(f"{CATALOG}.silver.map_level")
map_zone = spark.read.table(f"{CATALOG}.silver.map_zone")
weapon_attributes = spark.read.table(f"{CATALOG}.silver.weapon_attributes")


def write(df: DataFrame, table_name: str, comment: str) -> None:
    full_name = f"{CATALOG}.gold.{table_name}"
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")

# COMMAND ----------

write(
    rt.dim_map_reference(map_reference),
    "dim_map_reference",
    "Map dimension: radar transform, images, and metadata.",
)
write(
    rt.dim_map_level(map_level),
    "dim_map_level",
    "Map radar levels with altitude range and background image.",
)
write(
    rt.dim_map_zone(map_zone, map_reference),
    "dim_map_zone",
    "Bombsite and spawn zones in game world coordinates.",
)
write(
    rt.dim_weapon_spec(weapon_attributes),
    "dim_weapon_spec",
    "Weapon dimension; weapon_key matches the GSI pipeline dim_weapon.",
)
