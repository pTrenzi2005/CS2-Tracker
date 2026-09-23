# Databricks notebook source
# MAGIC %md
# MAGIC # Reference data (3/4) - Silver
# MAGIC
# MAGIC Cztery tabele wyciagniete z Bronze:
# MAGIC
# MAGIC | Tabela | Ziarno | Zrodlo |
# MAGIC | --- | --- | --- |
# MAGIC | `silver.map_reference` | mapa | `available.json` + plik overview |
# MAGIC | `silver.map_level` | mapa x poziom radaru | `verticalsections` |
# MAGIC | `silver.map_zone` | mapa x strefa | `bombA_*`, `bombB_*`, `CTSpawn_*`, `TSpawn_*` |
# MAGIC | `silver.weapon_attributes` | bron | arkusz `Raw Values` |
# MAGIC
# MAGIC Job nie ma expectations z Lakeflow, wiec kontrakty danych wymusza funkcja
# MAGIC `expect_all_or_drop` nizej - odsiewa te same rekordy i raportuje te same liczniki,
# MAGIC tylko do logu Joba zamiast do event logu potoku.

# COMMAND ----------

dbutils.widgets.text("catalog", "cs2_dev", "Katalog Unity Catalog")

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = dbutils.widgets.get("catalog")
MAP_ICONS_RAW = f"{CATALOG}.bronze.map_icons_raw"
MAP_ASSETS_RAW = f"{CATALOG}.bronze.map_assets_raw"
WEAPON_RAW = f"{CATALOG}.bronze.weapon_spreadsheet_raw"

INGEST_RUN_ID = dbutils.jobs.taskValues.get(taskKey="Download_data", key="ingest_run_id", debugValue="")
if not INGEST_RUN_ID:
    INGEST_RUN_ID = spark.read.table(MAP_ICONS_RAW).agg(F.max("ingest_run_id")).collect()[0][0]
print(f"ingest_run_id={INGEST_RUN_ID}")

# Bronze jest append-only, Silver opisuje wylacznie najswiezszy snapshot.
map_icons = spark.read.table(MAP_ICONS_RAW).filter(F.col("ingest_run_id") == INGEST_RUN_ID)
map_assets = spark.read.table(MAP_ASSETS_RAW).filter(F.col("ingest_run_id") == INGEST_RUN_ID)
weapon_raw = spark.read.table(WEAPON_RAW).filter(F.col("ingest_run_id") == INGEST_RUN_ID)


def expect_all_or_drop(df: DataFrame, table_name: str, rules: dict[str, str]) -> DataFrame:
    """Odpowiednik dekoratora `@dp.expect_all_or_drop` z potoku GSI.

    Warunek dajacy NULL liczy sie jako naruszenie, tak samo jak w Lakeflow - odsiewa go
    `filter`, bo NULL nie jest prawda.
    """
    violations = (
        df.agg(
            *[F.sum(F.when(F.expr(rule), 0).otherwise(1)).alias(name) for name, rule in rules.items()]
        )
        .collect()[0]
        .asDict()
    )
    failed = {name: count for name, count in violations.items() if count}
    if failed:
        print(f"{table_name}: odrzucone rekordy {failed}")
    condition = reduce(lambda left, right: left & right, [F.expr(rule) for rule in rules.values()])
    return df.filter(condition)


def write(df: DataFrame, table_name: str, comment: str) -> None:
    """Snapshot danych referencyjnych - nadpisanie w calosci jest idempotentne."""
    full_name = f"{CATALOG}.silver.{table_name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
    print(f"silver.{table_name}: {spark.read.table(full_name).count()} wierszy")


def blank_to_null(column, cast_to: str):
    """`regexp_extract` bez dopasowania zwraca pusty string - to brak klucza, nie zero."""
    return F.when(column == "", None).otherwise(column).cast(cast_to)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifest map
# MAGIC
# MAGIC Klucze obiektu `maps` to nazwy map, wiec schemat nie moze byc structem o stalych
# MAGIC polach - uzywamy `MAP<STRING, STRUCT<...>>` i rozwijamy go `explode`. Tak samo
# MAGIC `verticalsections`, gdzie kluczem jest nazwa poziomu (`default`, `lower`).

# COMMAND ----------

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

manifest_maps = (
    map_icons.filter(F.col("artifact_type") == "map_manifest")
    .select("source_commit", F.from_json("raw_content", MANIFEST_SCHEMA).alias("manifest"))
    .select("source_commit", F.explode("manifest.maps").alias("map_name", "entry"))
)
print(f"map w manifescie: {manifest_maps.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pliki overview
# MAGIC
# MAGIC Format KeyValues Valve'a: `"klucz"<tab>"wartosc"  // komentarz`. Wyciagamy z niego
# MAGIC tylko to, czego nie ma w `available.json`, czyli strefy plantu, respawny i insety.

# COMMAND ----------

radar_text = map_icons.filter(F.col("artifact_type") == "radar_info").select(
    "map_name", "raw_content"
)


def key_value(key: str):
    """Liczba spod klucza `key` w pliku overview, albo NULL gdy mapa go nie ma."""
    pattern = f'(?i)"{key}"\\s*"\\s*(-?[0-9]+(?:\\.[0-9]+)?)\\s*"'
    return blank_to_null(F.regexp_extract(F.col("raw_content"), pattern, 1), "double")


# Mapy spolecznosciowe wygenerowane RadGenem nie maja ani klucza `material`, ani stref -
# dla nich wszystkie ponizsze kolumny beda puste i tak ma byc.
overview = radar_text.select(
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_reference`

# COMMAND ----------

icons = map_assets.filter(F.col("asset_type") == "icon").select(
    "map_name", F.col("volume_path").alias("icon_volume_path")
)

prefix = F.split(F.col("map_name"), "_").getItem(0)
radar = F.col("entry.radar_info")
# size() na NULL-owej mapie zwraca -1 albo NULL zaleznie od trybu ANSI, wiec brak sekcji
# rozstrzygamy jawnie przez isNull().
section_count = (
    F.when(radar["verticalsections"].isNull(), F.lit(0))
    .otherwise(F.size(radar["verticalsections"]))
)

map_reference = (
    manifest_maps.join(overview, "map_name", "left")
    .join(icons, "map_name", "left")
    .select(
        "map_name",
        F.coalesce(F.col("entry.display_name"), F.col("map_name")).alias("display_name"),
        F.when(prefix == "de", "bomb_defusal")
        .when(prefix == "cs", "hostage")
        .when(prefix == "ar", "arms_race")
        .when(prefix == "dz", "danger_zone")
        .when(prefix == "gd", "guardian")
        .otherwise("other")
        .alias("map_family"),
        F.to_date("entry.first_seen").alias("first_seen_date"),
        F.col("entry.path").alias("icon_url"),
        "icon_volume_path",
        radar.isNotNull().alias("has_radar"),
        radar["pos_x"].alias("radar_pos_x"),
        radar["pos_y"].alias("radar_pos_y"),
        radar["scale"].alias("radar_scale"),
        # rotate i zoom wystepuja tylko na czesci map; brak wpisu znaczy "bez obrotu".
        F.coalesce(radar["rotate"], F.lit(0.0)).alias("radar_rotate"),
        F.coalesce(radar["zoom"], F.lit(1.0)).alias("radar_zoom"),
        "radar_material",
        F.when(section_count > 0, section_count).otherwise(F.lit(1)).alias("level_count"),
        F.coalesce(F.col("entry.radar_paths"), F.array().cast("array<string>")).alias("radar_urls"),
        F.coalesce(F.col("entry.thumb_paths"), F.array().cast("array<string>")).alias("thumb_urls"),
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
        F.lit(INGEST_RUN_ID).alias("ingest_run_id"),
        F.current_timestamp().alias("loaded_at"),
    )
)

map_reference = expect_all_or_drop(
    map_reference,
    "map_reference",
    {
        "znana_mapa": "map_name IS NOT NULL AND map_name <> ''",
        # Transformacja swiat -> piksel dzieli przez skale, wiec NULL albo zero psuje
        # kazda heatmape zbudowana na tej mapie.
        "kompletna_transformacja": (
            "NOT has_radar OR (radar_pos_x IS NOT NULL AND radar_pos_y IS NOT NULL "
            "AND radar_scale > 0)"
        ),
        "frakcje_w_zakresie": (
            "coalesce(bombsite_a_x, 0) BETWEEN 0 AND 1 AND coalesce(ct_spawn_x, 0) BETWEEN 0 AND 1"
        ),
    },
)
write(
    map_reference,
    "map_reference",
    "Metadane map z repozytorium cs2-map-icons: transformacja radaru, strefy, obrazy.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_level`
# MAGIC
# MAGIC Mapy wielopoziomowe (nuke, vertigo, boulder) maja osobny obraz radaru dla kazdej
# MAGIC wysokosci. Mapy jednopoziomowe dostaja sztuczny poziom `default` z zakresem
# MAGIC obejmujacym cala mape, zeby wyszukiwanie po `pos_z >= altitude_min AND pos_z <
# MAGIC altitude_max` dzialalo wszedzie tak samo i zawsze zwracalo jeden wiersz.

# COMMAND ----------

ALTITUDE_LIMIT = 100000.0

with_radar = manifest_maps.filter(radar.isNotNull()).select(
    "map_name",
    F.coalesce(F.col("entry.radar_paths"), F.array().cast("array<string>")).alias("radar_urls"),
    radar["verticalsections"].alias("sections"),
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

# Nazwa pliku radaru to <mapa>_radar z przyrostkiem formatu zrodlowego, a poziom inny niz
# `default` wchodzi miedzy nazwe mapy i slowo radar: de_nuke_lower_radar_psd.png. Przyrostek
# zalezy od tego, z czego Valve wygenerowalo obraz (_psd, _tga, albo nic), dlatego
# porownujemy nazwy po jego odcieciu, zamiast zakladac jeden wariant. Odsiewa to przy okazji
# warianty poboczne z tego samego repozytorium (_spectate_tga, kopie z hashem w nazwie).
IMAGE_SUFFIX = "(?i)(_(psd|tga|vtf|dds))?\\.png$"

expected_name = F.concat(
    F.col("map_name"),
    F.when(F.col("level_name") == "default", F.lit("")).otherwise(
        F.concat(F.lit("_"), F.col("level_name"))
    ),
    F.lit("_radar"),
)


def radar_base_name(url):
    return F.regexp_replace(F.element_at(F.split(url, "/"), -1), IMAGE_SUFFIX, "")


radar_assets = map_assets.filter(F.col("asset_type") == "radar").select(
    F.col("file_name").alias("radar_file_name"),
    F.col("volume_path").alias("radar_volume_path"),
)

map_level = (
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
        F.lit(INGEST_RUN_ID).alias("ingest_run_id"),
        F.current_timestamp().alias("loaded_at"),
    )
)

map_level = expect_all_or_drop(
    map_level,
    "map_level",
    {
        "znany_poziom": "map_name IS NOT NULL AND level_name IS NOT NULL",
        "zakres_wysokosci": "altitude_min < altitude_max",
        "znany_obraz": "radar_url IS NOT NULL",
    },
)
write(map_level, "map_level", "Poziomy radaru mapy z zakresem wysokosci i obrazem tla.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.map_zone`
# MAGIC
# MAGIC Wspolrzedne to ulamek boku obrazu radaru (0-1), nie jednostki swiata - przeliczenie
# MAGIC na wspolrzedne gry robi warstwa Gold, bo potrzebuje do tego skali i punktu zero.

# COMMAND ----------

ZONE_KEYS = {
    "bombsite_a": ("bombsite_a_x", "bombsite_a_y"),
    "bombsite_b": ("bombsite_b_x", "bombsite_b_y"),
    "ct_spawn": ("ct_spawn_x", "ct_spawn_y"),
    "t_spawn": ("t_spawn_x", "t_spawn_y"),
}

zone_frames = [
    overview.select(
        "map_name",
        F.lit(zone_type).alias("zone_type"),
        F.col(column_x).alias("frac_x"),
        F.col(column_y).alias("frac_y"),
    )
    for zone_type, (column_x, column_y) in ZONE_KEYS.items()
]

map_zone = (
    reduce(DataFrame.unionByName, zone_frames)
    .withColumn("ingest_run_id", F.lit(INGEST_RUN_ID))
    .withColumn("loaded_at", F.current_timestamp())
)

map_zone = expect_all_or_drop(
    map_zone,
    "map_zone",
    {
        # Mapy bez stref plantu (ar_, cs_) po prostu nie maja tych kluczy w pliku overview.
        "znana_pozycja": "frac_x IS NOT NULL AND frac_y IS NOT NULL",
        "frakcje_w_zakresie": "frac_x BETWEEN 0 AND 1 AND frac_y BETWEEN 0 AND 1",
    },
)
write(map_zone, "map_zone", "Strefy plantu i respawny jako ulamek boku obrazu radaru.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `silver.weapon_attributes`
# MAGIC
# MAGIC Arkusz `Raw Values` ma 53 kolumny i te same nazwy broni, ktorych uzywa GSI
# MAGIC (`ak47`, `m4a1_silencer`, `usp_silencer`), tylko bez przedrostka `weapon_`.
# MAGIC Arkusz `Simple` zostaje w Bronze - niesie te same liczby, ale w formie
# MAGIC prezentacyjnej (`$1,300`, `93.20%`) i z dodatkowymi wierszami wariantow
# MAGIC (`AUG (scoped)`), ktore nie maja odpowiednika w telemetrii.

# COMMAND ----------

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


def money(column: str):
    """`$1,300` i `$300` to ta sama kolumna w arkuszu - zostawiamy same cyfry."""
    return F.regexp_replace(F.col(column), "[^0-9]", "").cast("int")


raw_values = weapon_raw.filter(F.col("file_name") == "weapons_raw_values.csv")

is_header = F.col("raw_line").contains("WeaporArmorRatio")
is_blank = F.regexp_replace(F.col("raw_line"), ",", "").rlike("^\\s*$")
lines_in_order = Window.partitionBy("file_name").orderBy("line_number")

parsed = (
    # Naglowek powtarza sie nad kazdym blokiem i to on niesie nazwe kategorii w pierwszej
    # kolumnie ("Pistols", "Automatic Rifles", "LMGs"). Przenosimy ja w dol po numerze
    # linii, zanim naglowki zostana odsiane.
    raw_values.withColumn(
        "weapon_category",
        F.last(F.when(is_header, F.split(F.col("raw_line"), ",").getItem(0)), ignorenulls=True).over(
            lines_in_order
        ),
    )
    .filter(~is_header & ~is_blank)
    .withColumn("parsed", F.from_csv(F.col("raw_line"), WEAPON_CSV_SCHEMA))
    .select("line_number", "weapon_category", "ingest_run_id", "parsed.*")
)

weapon_attributes = parsed.select(
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

weapon_attributes = expect_all_or_drop(
    weapon_attributes,
    "weapon_attributes",
    {
        "znana_bron": "weapon_name IS NOT NULL AND weapon_name <> ''",
        "dodatnie_obrazenia": "damage > 0",
        "dodatni_takt": "cycle_time_s > 0",
        "znana_cena": "price_usd IS NOT NULL AND price_usd >= 0",
        "znany_typ": "weapon_type IS NOT NULL",
    },
)
write(
    weapon_attributes,
    "weapon_attributes",
    "Parametry broni z arkusza Raw Values datasetu Kaggle.",
)