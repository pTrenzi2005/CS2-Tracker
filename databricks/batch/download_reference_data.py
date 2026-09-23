# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (1/4) - pobranie zrodel wsadowych do Volume
# MAGIC
# MAGIC Jedyny task, ktory wychodzi po dane na zewnatrz. Zapisuje pliki **bajt w bajt**
# MAGIC do `/Volumes/<catalog>/bronze/raw/reference/` i opisuje kazdy z nich w manifescie.
# MAGIC Nastepne taski nie odpytuja juz internetu - czytaja Volume i manifest, dzieki czemu
# MAGIC caly przebieg Bronze -> Gold jest odtwarzalny bez dostepu do GitHuba i Kaggle.
# MAGIC
# MAGIC Zrodla:
# MAGIC - `MurkyYT/cs2-map-icons` - ikony map, radary, pliki overview (pos_x, pos_y, scale, strefy).
# MAGIC - Kaggle `billpureskillgg/cs2-weapon-spreadsheet` - parametry broni wyciagniete z plikow gry.

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Katalog Unity Catalog")
dbutils.widgets.text("map_icons_repo", "MurkyYT/cs2-map-icons", "Repozytorium z mapami")
dbutils.widgets.text("map_icons_ref", "main", "Branch albo tag repozytorium")
dbutils.widgets.text("kaggle_dataset", "billpureskillgg/cs2-weapon-spreadsheet", "Dataset Kaggle")
dbutils.widgets.dropdown("download_images", "true", ["true", "false"], "Pobierac PNG radarow i ikon")
dbutils.widgets.text("secret_scope", "cs2tracker-secrets", "Secret scope")

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

print(f"ingest_run_id={INGEST_RUN_ID}  root={ROOT}")

# COMMAND ----------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "cs2-tracker-reference-ingest"})


def http_get(url: str, *, auth=None, attempts: int = 4, timeout: int = 120) -> bytes:
    """GET z wykladniczym backoffem. Pobieramy ~150 malych plikow, wiec pojedynczy
    timeout albo 429 z GitHuba nie moze wywracac calego Joba."""
    last_error = None
    for attempt in range(attempts):
        try:
            response = SESSION.get(url, auth=auth, timeout=timeout)
            response.raise_for_status()
            return response.content
        except Exception as error:  # noqa: BLE001 - powod jest w komunikacie ponizej
            last_error = error
            if attempt < attempts - 1:
                time.sleep(2**attempt)
    raise RuntimeError(f"Nie udalo sie pobrac {url}: {last_error}")


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


def record(source_system, artifact_type, source_url, saved, *, map_name=None,
           asset_variant=None, source_file_name=None) -> dict:
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
# MAGIC ## Mapy: manifest, pliki overview, obrazy
# MAGIC
# MAGIC `available.json` opisuje wszystkie mapy w jednym zadaniu - stad bierzemy pos_x,
# MAGIC pos_y, scale i verticalsections. Surowe pliki `radar_info/<map>.txt` sciagamy mimo to,
# MAGIC bo tylko one niosa pozycje stref: `bombA_*`, `bombB_*`, `CTSpawn_*`, `TSpawn_*`.

# COMMAND ----------

available_url = f"{RAW_BASE}/data/available.json"
available_bytes = http_get(available_url)
files.append(record("cs2_map_icons", "map_manifest", available_url,
                    save("map_icons/available.json", available_bytes)))

available = json.loads(available_bytes)
maps = available["maps"]
print(f"map w manifescie: {len(maps)}")


def source_commit() -> str | None:
    """Commit, z ktorego pochodzi snapshot - lineage na wypadek zmiany danych po patchu Valve."""
    try:
        response = SESSION.get(f"https://api.github.com/repos/{REPO}/commits/{REF}", timeout=30)
        response.raise_for_status()
        return response.json().get("sha")
    except Exception as error:  # noqa: BLE001
        print(f"Nie udalo sie ustalic commita zrodlowego: {error}")
        return None


COMMIT = source_commit()
print(f"map_icons_commit={COMMIT}")

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

print(f"plikow radar_info: {len(radar_targets)} / {len(maps)} map")

# COMMAND ----------

def radar_variant(map_name: str, url: str) -> str:
    """Poziom radaru zakodowany w nazwie pliku: <mapa>[_<poziom>]_radar[_format].png.

    Przyrostek formatu zalezy od tego, z czego Valve wygenerowalo obraz, a repozytorium
    trzyma tez warianty poboczne (`_spectate`, kopie z hashem) - te trafiaja do `other`.
    """
    base = re.sub(r"(?i)(_(psd|tga|vtf|dds))?\.png$", "", os.path.basename(url))
    if base.startswith(f"{map_name}_") and base.endswith("_radar"):
        return base[len(map_name) + 1 : -len("_radar")] or "default"
    return "other"


# Ikony i radary ladujemy w Volume, zeby dashboard i AI Coach nie zalezaly od dostepnosci
# GitHuba. Miniatury pomijamy - ich URL-e i tak trafiaja do silver.map_reference.
image_targets: list[tuple[str, str, str, str]] = []
for name, entry in maps.items():
    if entry.get("path"):
        image_targets.append((name, "icon", entry["path"], "map_icons/images"))
    for url in entry.get("radar_paths") or []:
        image_targets.append((name, radar_variant(name, url), url, "map_icons/images/radars"))


def fetch_image(target: tuple[str, str, str, str]) -> dict:
    map_name, variant, url, directory = target
    saved = save(f"{directory}/{os.path.basename(url)}", http_get(url))
    return record("cs2_map_icons", "map_image", url, saved,
                  map_name=map_name, asset_variant=variant)


if DOWNLOAD_IMAGES:
    with ThreadPoolExecutor(max_workers=8) as pool:
        image_files = list(pool.map(fetch_image, image_targets))
    files.extend(image_files)
    print(f"obrazow: {len(image_files)} ({sum(f['size_bytes'] for f in image_files) / 1e6:.1f} MB)")
else:
    print("download_images=false - pomijam PNG, zostaja same URL-e")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronie: dataset Kaggle
# MAGIC
# MAGIC Dataset jest publiczny i schodzi bez logowania. Jesli Kaggle kiedys tego zabroni,
# MAGIC wystarczy dolozyc do secret scope klucze `kaggle-username` i `kaggle-key` -
# MAGIC kod uzyje ich automatycznie.

# COMMAND ----------


def kaggle_auth() -> tuple[str, str] | None:
    try:
        return (
            dbutils.secrets.get(scope=SECRET_SCOPE, key="kaggle-username"),
            dbutils.secrets.get(scope=SECRET_SCOPE, key="kaggle-key"),
        )
    except Exception:  # noqa: BLE001 - brak kluczy to normalny przypadek
        return None


def dataset_file_name(member: str) -> str:
    """Nazwy w archiwum maja spacje i nawiasy - na Volume zapisujemy wersje techniczna."""
    lowered = member.lower()
    if "raw values" in lowered:
        return "weapons_raw_values.csv"
    if "simple" in lowered:
        return "weapons_simple.csv"
    return re.sub(r"[^a-z0-9._-]+", "_", os.path.basename(lowered)).strip("_")


kaggle_url = f"https://www.kaggle.com/api/v1/datasets/download/{KAGGLE_DATASET}"
archive_bytes = http_get(kaggle_url, auth=kaggle_auth())

if not zipfile.is_zipfile(io.BytesIO(archive_bytes)):
    raise RuntimeError(
        f"Kaggle zwrocil {len(archive_bytes)} B, ktore nie sa archiwum ZIP - najpewniej "
        f"strone logowania. Dodaj klucze kaggle-username i kaggle-key do scope {SECRET_SCOPE}."
    )

with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
    for member in archive.namelist():
        if member.endswith("/"):
            continue
        saved = save(f"weapons/{dataset_file_name(member)}", archive.read(member))
        files.append(record("kaggle_weapon_spreadsheet", "weapon_csv", kaggle_url, saved,
                            source_file_name=member))
        print(f"{member} -> {saved['file_name']} ({saved['size_bytes']} B)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifest
# MAGIC
# MAGIC Warstwa Bronze czyta wlasnie ten plik zamiast przeszukiwac Volume - dzieki temu
# MAGIC wie, skad wzial sie kazdy bajt, i nie wciagnie plikow z poprzedniego przebiegu.

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
# Wskaznik na ostatni przebieg - pozwala uruchomic kolejne notebooki recznie, poza Jobem.
save("_manifest/latest.json", manifest_bytes)

by_type: dict[str, int] = {}
for item in files:
    by_type[item["artifact_type"]] = by_type.get(item["artifact_type"], 0) + 1
print(f"plikow lacznie: {len(files)}  {by_type}")

dbutils.jobs.taskValues.set(key="ingest_run_id", value=INGEST_RUN_ID)