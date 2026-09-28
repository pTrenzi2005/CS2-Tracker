# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data - consistency checks
# MAGIC
# MAGIC Cross-table checks on the gold reference snapshot. A failure stops the job.
# MAGIC Row-level rules already run in silver.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "OK  " if condition else "FAIL"
    print(f"{status} {name} {detail}")
    if not condition:
        failures.append(name)


def duplicates(table_name: str, keys: list[str]) -> int:
    df = spark.read.table(f"{CATALOG}.gold.{table_name}")
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()


maps = spark.read.table(f"{CATALOG}.gold.dim_map_reference")
levels = spark.read.table(f"{CATALOG}.gold.dim_map_level")
zones = spark.read.table(f"{CATALOG}.gold.dim_map_zone")
weapons = spark.read.table(f"{CATALOG}.gold.dim_weapon_spec")

check("unique map_key", duplicates("dim_map_reference", ["map_key"]) == 0)
check("unique map_key + level_name", duplicates("dim_map_level", ["map_key", "level_name"]) == 0)
check("unique map_key + zone_type", duplicates("dim_map_zone", ["map_key", "zone_type"]) == 0)
check("unique weapon_key", duplicates("dim_weapon_spec", ["weapon_key"]) == 0)

with_radar = maps.filter("has_radar").count()
with_level = levels.select("map_key").distinct().count()
check(
    "every radar map has a level",
    with_radar == with_level,
    f"({with_level}/{with_radar})",
)

orphan_levels = levels.join(maps, "map_key", "left_anti").count()
orphan_zones = zones.join(maps, "map_key", "left_anti").count()
check("levels belong to a map", orphan_levels == 0)
check("zones belong to a map", orphan_zones == 0)

check(
    "fire rate is realistic",
    weapons.filter("fire_rate_rpm <= 0 OR fire_rate_rpm > 2000").count() == 0,
)

if failures:
    raise AssertionError(f"Final checks failed: {failures}")
print("Final checks passed")
