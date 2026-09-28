# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (1/4) - download sources to Volume
# MAGIC
# MAGIC Writes files to `/Volumes/<catalog>/bronze/raw/reference/` and describes each one in the manifest.
# MAGIC
# MAGIC Sources:
# MAGIC - `MurkyYT/cs2-map-icons` - map icons, radars, overview files (pos_x, pos_y, scale, zones).
# MAGIC - Kaggle `billpureskillgg/cs2-weapon-spreadsheet` - weapon parameters extracted from game files.

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Unity Catalog")
dbutils.widgets.text("map_icons_repo", "MurkyYT/cs2-map-icons", "Repository with map icons")
dbutils.widgets.text("map_icons_ref", "main", "Branch or tag repository")
dbutils.widgets.text("kaggle_dataset", "billpureskillgg/cs2-weapon-spreadsheet", "Dataset Kaggle")
dbutils.widgets.dropdown("download_images", "true", ["true", "false"], "Download PNG radars and icons (optional)")
dbutils.widgets.text("secret_scope", "cs2tracker-secrets", "Secrets scope")

# COMMAND ----------

import hashlib
import io
import json
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

CATALOG = dbutils.widgets.get("catalog")
REPO = dbutils.widgets.get("map_icons_repo")
REF = dbutils.widgets.get("map_icons_ref")
KAGGLE_DATASET = dbutils.widgets.get("kaggle_dataset")
DOWNLOAD_IMAGES = dbutils.widgets.get("download_images").lower() == "true"
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

ROOT = f"/Volumes/{CATALOG}/bronze/raw"
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/{REF}"

RUN_TS = datetime.now(timezone.utc)
INGEST_RUN_ID = RUN_TS.strftime("%Y%m%dT%H%M%SZ")

#print(f"ingest_run_id={INGEST_RUN_ID}  root={ROOT}")

# COMMAND ----------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "cs2-tracker-reference-ingest"})


def http_get(url: str, *, auth=None, attempts: int = 4, timeout: int = 120) -> bytes:
    last_error = None

    for attempt in range(attempts):
        try:
            response = SESSION.get(url, auth=auth, timeout=timeout)
            response.raise_for_status()
            return response.content
        except Exception as error:
            last_error = error
            if attempt < attempts - 1:
                time.sleep(2**attempt)

    raise RuntimeError(f"Failed to download {url}: {last_error}")


def save(relative_path: str, content: bytes) -> dict:
    target = f"{ROOT}/{relative_path}"
    os.makedirs(os.path.dirname(target), exist_ok=True)

    with open(target, "wb") as handle:
        handle.write(content)

    return {
        "volume_path": target,
        "file_name": os.path.basename(target),
        "size_bytes": len(content),
        "content_sha256": hashlib.sha256(content).hexdigest(),
    }


def record(source_system, artifact_type, source_url, saved, *, map_name=None, asset_variant=None, source_file_name=None) -> dict:
    return {
        "source_system": source_system,
        "artifact_type": artifact_type,
        "map_name": map_name,
        "asset_variant": asset_variant,
        "source_url": source_url,
        "source_file_name": source_file_name or os.path.basename(source_url),
        **saved,
    }


files: list[dict] = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maps: manifest, overview files, images
# MAGIC Source - `MurkyYT/cs2-map-icons`:
# MAGIC
# MAGIC `available.json` describes all maps in one task - that's where we get pos_x, pos_y, scale and verticalsections.

# COMMAND ----------

available_url = f"{RAW_BASE}/data/available.json"
available_bytes = http_get(available_url)
files.append(record("cs2_map_icons", "map_manifest", available_url,
                    save("map_icons/available.json", available_bytes)))

available = json.loads(available_bytes)
maps = available["maps"]

def source_commit() -> str | None:
    try:
        response = SESSION.get(f"https://api.github.com/repos/{REPO}/commits/{REF}", timeout=30)
        response.raise_for_status()
        return response.json().get("sha")
    except Exception as error:
        print(f"Failed to determine source commit: {error}")
        return None


COMMIT = source_commit()

# COMMAND ----------

radar_targets = [
    (name, entry["radar_info"]["path"])
    for name, entry in maps.items()
    if (entry.get("radar_info") or {}).get("path")
]


def fetch_radar_info(target: tuple[str, str]) -> dict:
    map_name, url = target
    saved = save(f"map_icons/radar_info/{os.path.basename(url)}", http_get(url))
    return record("cs2_map_icons", "radar_info", url, saved, map_name=map_name)


with ThreadPoolExecutor(max_workers=8) as pool:
    files.extend(pool.map(fetch_radar_info, radar_targets))

# COMMAND ----------

def radar_variant(map_name: str, url: str) -> str:
    base = re.sub(r"(?i)(_(psd|tga|vtf|dds))?\.png$", "", os.path.basename(url))
    if base.startswith(f"{map_name}_") and base.endswith("_radar"):
        return base[len(map_name) + 1 : -len("_radar")] or "default"
    return "other"


# Icons and radars are stored in Volume
image_targets: list[tuple[str, str, str, str]] = []
for name, entry in maps.items():
    if entry.get("path"):
        image_targets.append((name, "icon", entry["path"], "map_icons/images"))
    for url in entry.get("radar_paths") or []:
        image_targets.append((name, radar_variant(name, url), url, "map_icons/images/radars"))


def fetch_image(target: tuple[str, str, str, str]) -> dict:
    map_name, variant, url, directory = target
    saved = save(f"{directory}/{os.path.basename(url)}", http_get(url))
    return record("cs2_map_icons", "map_image", url, saved, map_name=map_name, asset_variant=variant)


if DOWNLOAD_IMAGES:
    with ThreadPoolExecutor(max_workers=8) as pool:
        image_files = list(pool.map(fetch_image, image_targets))
    files.extend(image_files)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Weapons: weapons spreadsheet, statistics
# MAGIC Source - Kaggle `billpureskillgg/cs2-weapon-spreadsheet`:
# MAGIC

# COMMAND ----------


def dataset_file_name(member: str) -> str:
    lowered = member.lower()
    if "raw values" in lowered:
        return "weapons_raw_values.csv"
    if "simple" in lowered:
        return "weapons_simple.csv"
    return re.sub(r"[^a-z0-9._-]+", "_", os.path.basename(lowered)).strip("_")


kaggle_url = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"
archive_bytes = http_get(kaggle_url)


with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
    for member in archive.namelist():
        if member.endswith("/"):
            continue

        saved = save(f"weapons/{dataset_file_name(member)}", archive.read(member))
        files.append(record("kaggle_weapon_spreadsheet", "weapon_csv", kaggle_url, saved,
                            source_file_name=member))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifest
# MAGIC
# MAGIC Bronze layer reads this file instead of searching Volume - this way it knows where each byte came from and doesn't include files from previous runs.

# COMMAND ----------

manifest = {
    "ingest_run_id": INGEST_RUN_ID,
    "downloaded_at": RUN_TS.strftime("%Y-%m-%d %H:%M:%S"),
    "map_icons_repo": REPO,
    "map_icons_ref": REF,
    "map_icons_commit": COMMIT,
    "kaggle_dataset": KAGGLE_DATASET,
    "download_images": DOWNLOAD_IMAGES,
    "files": files,
}
manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
save(f"_manifest/{INGEST_RUN_ID}.json", manifest_bytes)

save("_manifest/latest.json", manifest_bytes)

by_type: dict[str, int] = {}
for item in files:
    by_type[item["artifact_type"]] = by_type.get(item["artifact_type"], 0) + 1

dbutils.jobs.taskValues.set(key="ingest_run_id", value=INGEST_RUN_ID)