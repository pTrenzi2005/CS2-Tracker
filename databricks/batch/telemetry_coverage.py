# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data - telemetry coverage
# MAGIC
# MAGIC Compares GSI dimensions with the reference snapshot. Informational only.
# MAGIC Knives, grenades, C4, and the taser are absent from the Kaggle sheet.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")

maps = spark.read.table(f"{CATALOG}.gold.dim_map_reference")
weapons = spark.read.table(f"{CATALOG}.gold.dim_weapon_spec")

for table, key, label in [
    ("dim_weapon", "weapon_key", "weapons"),
    ("dim_map", "map_key", "maps"),
]:
    full_name = f"{CATALOG}.gold.{table}"
    if not spark.catalog.tableExists(full_name):
        print(f"{full_name} does not exist — the GSI pipeline has not run yet")
        continue
    observed = spark.read.table(full_name).select(key).distinct()
    reference = (weapons if key == "weapon_key" else maps).select(key).distinct()
    total = observed.count()
    matched = observed.join(reference, key, "inner").count()
    print(f"{label} in telemetry: {matched}/{total} have a reference match")
    if matched < total:
        name_column = "weapon_name" if key == "weapon_key" else "map_name"
        missing = spark.read.table(full_name).join(reference, key, "left_anti")
        display(missing.select(key, name_column).limit(20))
