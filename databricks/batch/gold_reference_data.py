# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (4/4) - Gold
# MAGIC
# MAGIC Wymiary danych wsadowych, kluczowane **tymi samymi funkcjami skrotu**, co schemat
# MAGIC gwiazdy potoku GSI:
# MAGIC
# MAGIC - `map_key = xxhash64(map_name)` - tak jak w `gold.dim_map` i w kazdej tabeli faktow,
# MAGIC - `weapon_key = xxhash64(weapon_name)`, gdzie `weapon_name` to nazwa z GSI
# MAGIC   (`weapon_ak47`), czyli arkusz Kaggle dostaje przedrostek `weapon_`.
# MAGIC
# MAGIC Dzieki temu `fact_kills` laczy sie z `dim_weapon_spec` bez zadnego mostka, a Job nie
# MAGIC dotyka tabel, ktorych wlascicielem jest potok Lakeflow.
# MAGIC
# MAGIC | Tabela | Ziarno |
# MAGIC | --- | --- |
# MAGIC | `gold.dim_map_reference` | mapa |
# MAGIC | `gold.dim_map_level` | mapa x poziom radaru |
# MAGIC | `gold.dim_map_zone` | mapa x strefa |
# MAGIC | `gold.dim_weapon_spec` | bron |

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Katalog Unity Catalog")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = dbutils.widgets.get("catalog")

# Radary CS2 to kwadratowe obrazy 1024 x 1024 piksele. Przeliczenie pozycji z telemetrii
# na piksel radaru:
#   pixel_x = (pos_x - radar_pos_x) / radar_scale
#   pixel_y = (radar_pos_y - pos_y) / radar_scale
# Os Y jest odwrocona, bo radar liczy piksele od gornej krawedzi, a gra od srodka mapy.
RADAR_SIZE_PX = 1024

map_reference = spark.read.table(f"{CATALOG}.silver.map_reference")
map_level = spark.read.table(f"{CATALOG}.silver.map_level")
map_zone = spark.read.table(f"{CATALOG}.silver.map_zone")
weapon_attributes = spark.read.table(f"{CATALOG}.silver.weapon_attributes")

map_key = F.xxhash64(F.col("map_name")).alias("map_key")


def write(df: DataFrame, table_name: str, comment: str) -> None:
    full_name = f"{CATALOG}.gold.{table_name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
    print(f"gold.{table_name}: {spark.read.table(full_name).count()} wierszy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_map_reference`
# MAGIC
# MAGIC Poza surowymi parametrami radaru tabela niesie gotowy prostokat mapy we
# MAGIC wspolrzednych swiata. Dashboard nie musi wtedy znac wzoru - wystarczy, ze
# MAGIC znormalizuje `pos_x` / `pos_y` do tego zakresu.

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
    "Wymiar mapy z danych wsadowych: transformacja radaru, obrazy i metadane.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_map_level`
# MAGIC
# MAGIC Wybor wlasciwego obrazu radaru dla danej wysokosci:
# MAGIC `JOIN ... ON f.map_key = l.map_key AND f.pos_z >= l.altitude_min AND f.pos_z < l.altitude_max`.
# MAGIC Przedzial domkniety tylko z dolu, bo sasiednie poziomy dziela granice (na de_nuke
# MAGIC jest nia -495) i `BETWEEN` zwrocilby dla niej dwa wiersze. Na mapach
# MAGIC jednopoziomowych zapytanie zawsze zwraca dokladnie jeden wiersz.

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

write(dim_map_level, "dim_map_level", "Poziomy radaru mapy z zakresem wysokosci i obrazem tla.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_map_zone`
# MAGIC
# MAGIC Strefy plantu i respawny przeliczone z ulamka obrazu na wspolrzedne swiata, czyli
# MAGIC na te sama skale, w ktorej przychodza pozycje gracza z GSI. Pozwala to liczyc
# MAGIC odleglosc od bombsite'a wprost w SQL-u.

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
    "Strefy plantu i respawny we wspolrzednych swiata gry.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_weapon_spec`
# MAGIC
# MAGIC Kilka wskaznikow liczymy tutaj, bo w arkuszu sa tylko wartosci surowe, a dashboard
# MAGIC i AI Coach potrzebuja liczb, ktorymi da sie porownywac bronie wprost.

# COMMAND ----------

cycle_time = F.col("cycle_time_s")
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
    # Bez uwzglednienia kasku, kamizelki i spadku obrazen na dystansie - to skrot
    # informacyjny, nie model balistyki.
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
    "Wymiar broni z parametrami z plikow gry; weapon_key zgodny z dim_weapon potoku GSI.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kontrola koncowa
# MAGIC
# MAGIC Job nie ma event logu potoku, wiec niespojnosc musi wywrocic przebieg tutaj.

# COMMAND ----------

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "OK  " if condition else "BLAD"
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

check("unikalny map_key", duplicates("dim_map_reference", ["map_key"]) == 0)
check("unikalny map_key + level_name", duplicates("dim_map_level", ["map_key", "level_name"]) == 0)
check("unikalny map_key + zone_type", duplicates("dim_map_zone", ["map_key", "zone_type"]) == 0)
check("unikalny weapon_key", duplicates("dim_weapon_spec", ["weapon_key"]) == 0)

with_radar = maps.filter("has_radar").count()
with_level = levels.select("map_key").distinct().count()
check(
    "kazda mapa z radarem ma poziom",
    with_radar == with_level,
    f"({with_level}/{with_radar})",
)

orphan_levels = levels.join(maps, "map_key", "left_anti").count()
orphan_zones = zones.join(maps, "map_key", "left_anti").count()
check("poziomy maja swoja mape", orphan_levels == 0)
check("strefy maja swoja mape", orphan_zones == 0)

check(
    "transformacja radaru kompletna",
    maps.filter("has_radar AND (radar_scale IS NULL OR radar_scale <= 0)").count() == 0,
)
check("kazda bron ma cene i obrazenia", weapons.filter("price_usd IS NULL OR damage <= 0").count() == 0)
check("realny fire rate", weapons.filter("fire_rate_rpm <= 0 OR fire_rate_rpm > 2000").count() == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pokrycie telemetrii
# MAGIC
# MAGIC Informacyjnie, nie blokujaco. Noze, granaty, C4 i taser nie wystepuja w arkuszu
# MAGIC Kaggle - dla nich `dim_weapon_spec` bedzie pusty i tak ma byc.

# COMMAND ----------

for table, key, label in [
    ("dim_weapon", "weapon_key", "broni"),
    ("dim_map", "map_key", "map"),
]:
    full_name = f"{CATALOG}.gold.{table}"
    if not spark.catalog.tableExists(full_name):
        print(f"{full_name} nie istnieje - potok GSI jeszcze nie byl uruchomiony")
        continue
    observed = spark.read.table(full_name).select(key).distinct()
    reference = (weapons if key == "weapon_key" else maps).select(key).distinct()
    total = observed.count()
    matched = observed.join(reference, key, "inner").count()
    print(f"{label} w telemetrii: {matched}/{total} ma odpowiednik w danych wsadowych")
    if matched < total:
        name_column = "weapon_name" if key == "weapon_key" else "map_name"
        missing = spark.read.table(full_name).join(reference, key, "left_anti")
        display(missing.select(key, name_column).limit(20))

# COMMAND ----------

if failures:
    raise AssertionError(f"Kontrola koncowa nie przeszla: {failures}")
print("Kontrola koncowa: wszystko przeszlo")
