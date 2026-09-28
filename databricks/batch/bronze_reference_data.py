# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (2/4) - Bronze
# MAGIC
# MAGIC Append-only bronze tables for ingest snapshot (`ingest_run_id`):
# MAGIC - `bronze.map_icons_raw` — map manifest and radar overview text
# MAGIC - `bronze.weapon_spreadsheet_raw` — weapon spreadsheet lines
# MAGIC - `bronze.map_assets_raw` — map image metadata (path, size, hash)
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")

# COMMAND ----------

import json
import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = dbutils.widgets.get("catalog")
ROOT = f"/Volumes/{CATALOG}/bronze/raw"

#identyfing the ingest run id
INGEST_RUN_ID = dbutils.jobs.taskValues.get(taskKey="Download_data", key="ingest_run_id", debugValue="")
if not INGEST_RUN_ID:
    with open(f"{ROOT}/_manifest/latest.json", "r", encoding="utf-8") as handle:
        INGEST_RUN_ID = json.load(handle)["ingest_run_id"]

MANIFEST_PATH = f"{ROOT}/_manifest/{INGEST_RUN_ID}.json"

# COMMAND ----------

FILE_DDL = (
    "source_system STRING, artifact_type STRING, map_name STRING, asset_variant STRING, "
    "source_url STRING, source_file_name STRING, volume_path STRING, file_name STRING, "
    "size_bytes LONG, content_sha256 STRING"
)
MANIFEST_DDL = (
    "ingest_run_id STRING, downloaded_at STRING, map_icons_repo STRING, map_icons_ref STRING, "
    "map_icons_commit STRING, kaggle_dataset STRING, download_images BOOLEAN, "
    f"files ARRAY<STRUCT<{FILE_DDL}>>"
)

manifest = (
    spark.read.schema(MANIFEST_DDL)
    .option("multiLine", True)
    .json(MANIFEST_PATH)
    .select(
        "ingest_run_id",
        F.to_timestamp("downloaded_at").alias("downloaded_at"),
        F.col("map_icons_ref").alias("source_ref"),
        F.col("map_icons_commit").alias("source_commit"),
        F.explode("files").alias("file"),
    )
    .select("ingest_run_id", "downloaded_at", "source_ref", "source_commit", "file.*")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## `bronze.map_icons_raw` - maps manifest and radar overview info
# MAGIC

# COMMAND ----------

text_paths = [f"{ROOT}/map_icons/available.json"]
if os.path.isdir(f"{ROOT}/map_icons/radar_info"):
    text_paths.append(f"{ROOT}/map_icons/radar_info")

text_files = (
    spark.read.text(text_paths, wholetext=True)
    .select(
        F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1).alias("file_name"),
        F.col("value").alias("raw_content"),
    )
)

map_icons_raw = manifest.filter(F.col("artifact_type").isin("map_manifest", "radar_info"))
    .join(text_files, "file_name", "inner")
        .select(
            "ingest_run_id",
            "downloaded_at",
            "source_system",
            "artifact_type",
            "map_name",
            "source_url",
            "source_ref",
            "source_commit",
            "volume_path",
            "file_name",
            "raw_content",
            "content_sha256",
            "size_bytes",
        )

(
    map_icons_raw.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.map_icons_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `bronze.weapon_spreadsheet_raw` - weapon spreadsheet with statictics
# MAGIC

# COMMAND ----------

by_file = Window.partitionBy("file_name").orderBy("_read_order")

weapon_lines = (
    spark.read.text(f"{ROOT}/weapons")
    .select(
        F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1).alias("file_name"),
        F.col("value").alias("raw_line"),
    )
    .withColumn("_read_order", F.monotonically_increasing_id())
    .withColumn("line_number", F.row_number().over(by_file))
    .drop("_read_order")
)

weapon_spreadsheet_raw = manifest.filter(F.col("artifact_type") == "weapon_csv")
    .join(weapon_lines, "file_name", "inner")
        .select(
            "ingest_run_id",
            "downloaded_at",
            "source_system",
            "source_url",
            "source_file_name",
            "volume_path",
            "file_name",
            "line_number",
            "raw_line",
            "content_sha256",
        )

(
    weapon_spreadsheet_raw.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.weapon_spreadsheet_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `bronze.map_assets_raw` - holds map assets metadata (path, size, hash)
# MAGIC

# COMMAND ----------

map_assets_raw = manifest.filter(F.col("artifact_type") == "map_image")
    .select(
        "ingest_run_id",
        "downloaded_at",
        "map_name",
        F.when(F.col("volume_path").contains("/radars/"), "radar")
        .otherwise("icon")
        .alias("asset_type"),
        F.col("asset_variant").alias("asset_variant"),
        "source_url",
        "source_ref",
        "source_commit",
        "volume_path",
        "file_name",
        "content_sha256",
        "size_bytes",
    )

(
    map_assets_raw.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.map_assets_raw")
)