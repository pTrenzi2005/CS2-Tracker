# Databricks

Two workloads share one Unity Catalog and one Asset Bundle. They do not share table ownership.

| Workload | What it is | When it runs |
| --- | --- | --- |
| GSI medallion pipeline | Lakeflow Declarative Pipeline `cs2_gsi_medallion` | After matches, on demand (`continuous: false`) |
| Reference data job | Job `cs2_reference_data_ingest` | Once, then again after a game update |

The pipeline turns live match telemetry into a star schema. The job loads static map and weapon data and keeps its own gold dimensions. Facts join those dimensions on `map_key` and `weapon_key`.

Bundle definition: [`databricks.yml`](../databricks.yml). Dev publishes to catalog `cs2_dev`. Prod publishes to `cs2_prod`.

# Layout

```text
databricks/
  01_architecture_setup.ipynb   Unity Catalog schemas and the bronze volume
  pipelines/                     GSI pipeline (Lakeflow Python)
    bronze/bronze_gsi_events.py
    silver/silver_events_parsed.py
    silver/silver_ticks.py
    silver/silver_round_events.py
    silver/silver_derived_events.py
    gold/gold_dimensions.py
    gold/gold_facts.py
  batch/                         Reference-data notebooks
    download_reference_data.py
    bronze_reference_data.py
    silver_reference_data.py
    gold_reference_data.py
    validate_reference_data.py
    telemetry_coverage.py        manual; not a job task
  resources/
    cs2_medallion.pipeline.yml
    cs2_reference_data.job.yml
```

`pipelines/` files are pipeline libraries. `batch/` files are Databricks notebooks.

# Medallion architecture

Both workloads use the same three schemas. The meaning of each layer is the same; the write mode is not.

| Layer | Role | GSI pipeline | Reference job |
| --- | --- | --- | --- |
| Bronze | Store the source payload with ingest metadata. Do not interpret it. | Streaming table, append from Kafka | Append-only tables, one snapshot per `ingest_run_id` |
| Silver | Parse, type, and drop bad rows. One business event per row. | Streaming tables, plus materialized views where `lag()` needs the whole partition | Overwrite of the latest snapshot |
| Gold | Star schema for dashboards: dimensions, facts, and one match aggregate | Materialized views, rebuilt from silver | Overwrite of reference dimensions only |

A Lakeflow table belongs to the pipeline. Rebuilding the pipeline drops it. Reference tables are written by the job, so they survive a pipeline rebuild. That is why map and weapon reference data is a job, not a second pipeline: the source is a full HTTP download, there is nothing to increment, and the tables must outlive the streaming pipeline.

# Data sources

**Match telemetry.** The local GSI client sends JSON events to Azure Event Hubs. Bronze reads the hub as a Kafka topic. The connection string lives in secret scope `cs2tracker-secrets`, key `evh-connection-string`.

Event types kept by silver: `tick`, `round_start`, `round_end`, `match_start`, `match_end`. GSI sends state, not kill or damage events. Kills, deaths, and damage are derived from counter changes between ticks.

**Map reference.** [MurkyYT/cs2-map-icons](https://github.com/MurkyYT/cs2-map-icons): `available.json` (radar transform and altitude levels), overview files (plant zones and spawns), and PNG icons and radars.

**Weapon reference.** [Kaggle `billpureskillgg/cs2-weapon-spreadsheet`](https://www.kaggle.com/datasets/billpureskillgg/cs2-weapon-spreadsheet). Silver uses the Raw Values sheet. The Simple sheet stays in bronze. The public download does not require a Kaggle login. If that changes, add `kaggle-username` and `kaggle-key` to the same secret scope.

# Shared keys

Gold on both sides uses the same surrogate keys, so a fact can join a reference dimension without a bridge table.

| Key | Expression | Used on |
| --- | --- | --- |
| `map_key` | `xxhash64(map_name)` | Maps |
| `weapon_key` | `xxhash64(weapon_name)` | Weapons. Reference data prefixes the sheet name with `weapon_` so it matches GSI (`weapon_ak47`) |
| `player_key` | `sha2(steamid, 256)` | Players. Raw SteamID64 stops in silver |
| `match_key` | `xxhash64(match_id)` | Matches |
| `round_key` | `xxhash64(match_id, round_number)` | Rounds |

`gold.dim_map` and `gold.dim_weapon` belong to the GSI pipeline and list only what telemetry actually saw. The job never writes those tables. It writes `gold.dim_map_reference` and `gold.dim_weapon_spec` instead.

# Setup

[`01_architecture_setup.ipynb`](01_architecture_setup.ipynb) creates schemas `bronze`, `silver`, `gold`, and `metastore`, plus the external volume `bronze.raw`. Run it once per catalog before the first deploy.

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      cs2_medallion -t dev
databricks bundle run      cs2_reference_data -t dev
```

Pipeline settings (catalog, Event Hubs namespace, secret scope, starting offset) are bundle variables. Pipeline code reads them with `spark.conf.get`. Job notebooks read the same catalog from a widget.

# GSI pipeline

Defined in [`resources/cs2_medallion.pipeline.yml`](resources/cs2_medallion.pipeline.yml). Serverless, triggered, channel `CURRENT`. Every table uses a fully qualified name, so one pipeline publishes into `bronze`, `silver`, and `gold`.

### 1. `pipelines/bronze/bronze_gsi_events.py`

Reads the Event Hubs Kafka topic. The JSON body is stored as text.

| Table | Grain | Data |
| --- | --- | --- |
| `bronze.gsi_raw_events` | One Kafka message | `raw_payload`, topic, partition, offset, Kafka timestamp, `ingest_ts`, `ingest_date` |

### 2. `pipelines/silver/silver_events_parsed.py`

Parses the envelope once. Later silver tables read the struct instead of repeating the JSON schema. Event Hubs is at-least-once, so duplicates are dropped inside a 2-hour watermark on `event_id`.

| Table | Grain | Data |
| --- | --- | --- |
| `silver.gsi_events_parsed` | One event | Envelope fields (`event_type`, `match_id`, `seq`, `client_ts`, `steamid`, `round_number`) and the typed `payload` |

Dropped rows: missing `event_id`, `match_id`, `steamid`, or `client_ts`, or an unknown `event_type`.

### 3. `pipelines/silver/silver_ticks.py`

Keeps ticks from the active round and splits GSI position strings (`"x, y, z"`) into coordinates and view angles.

| Table | Grain | Data |
| --- | --- | --- |
| `silver.player_ticks` | One tick | Map, score, player state, weapons, bomb, position, yaw and pitch |

Dropped rows: bad round number, health, damage, position, or team.

### 4. `pipelines/silver/silver_round_events.py`

Round and match boundaries. `win_team` exists only on `round_end`, which is not a tick, so gold cannot learn the winner from ticks alone.

| Table | Grain | Data |
| --- | --- | --- |
| `silver.round_events` | `round_start` or `round_end` | Scores, winner, win reason, player state and position |
| `silver.match_events` | `match_start` or `match_end` | Map, scores, rounds played |

### 5. `pipelines/silver/silver_derived_events.py`

Materialized views. `lag()` over a round cannot run as an incremental stream, and a full recompute of one match is cheap.

| Table | Grain | Data |
| --- | --- | --- |
| `silver.kill_events` | Tick where the kill counter increased | Kill count, headshots, weapon, position. One sample can hold more than one kill |
| `silver.death_events` | One death per round | Position and health before death. A tick wins over `round_end` when both exist |
| `silver.damage_events` | One damage delta | `dealt` from `round_totaldmg`, `taken` from the health drop. Armor absorption is not in `taken` |
| `silver.round_player_state` | Round | Time tracked, distance, economy, spawn, peak equip value |

The lag window is the round, not the match. A match-level window would pin a last-second kill on the next round's spawn.

### 6. `pipelines/gold/gold_dimensions.py`

| Table | Grain | Data |
| --- | --- | --- |
| `gold.dim_player` | Player | Masked SteamID, name, first and last seen, matches played |
| `gold.dim_map` | Map seen in telemetry | `map_key`, name, matches played |
| `gold.dim_weapon` | Weapon seen in inventory | `weapon_key`, type, class (`primary`, `sidearm`, `utility`, …) |
| `gold.dim_round` | Round | Times, winner, economy, `round_type` (`eco`, `force`, `full`), pistol-round flag |
| `gold.dim_match` | Match | Duration, rounds won and lost, result, rounds won on CT and on T |

`round_type` uses the peak equip value during the round. The `round_start` packet arrives in freeze time, before buys. A pistol round is round 0 or the first round after the player changes side, so the rule works for MR12 and for wingman.

### 7. `pipelines/gold/gold_facts.py`

| Table | Grain | Data |
| --- | --- | --- |
| `gold.fact_player_positions` | Tick | Path through the round: position, aim, health, weapon, flash, smoke, fire, `time_in_round_s` |
| `gold.fact_kills` | Kill sample | Place, weapon, headshots, time in round |
| `gold.fact_deaths` | Death | Place, time alive, whether it was detected from a tick or from `round_end` |
| `gold.fact_damage` | Damage delta | Dealt or taken, weapon, place, time in round |
| `gold.fact_round_stats` | Round and player | Kills, deaths, damage, survival, distance. Source for K/D, ADR, and headshot rate |
| `gold.agg_match_summary` | Match and player | Match KPIs: K/D, ADR, headshot percent, survival percent |

Ticks stop when the player dies, because the camera leaves them. `fact_kills`, `fact_deaths`, and `fact_damage` only contain samples that have a position, so they can miss the last fraction of a second. `fact_round_stats` takes the final counters from `round_end` and is the source of truth for round totals. `kills_with_position` is the part of those kills that can be drawn on a map.

# Reference data job

Defined in [`resources/cs2_reference_data.job.yml`](resources/cs2_reference_data.job.yml). Five tasks, in order: `download`, `bronze`, `silver`, `gold`, `validate`. No schedule. Run it again when map or weapon data changes.

Download writes files to `/Volumes/<catalog>/bronze/raw/`:

```text
map_icons/available.json
map_icons/radar_info/
map_icons/images/            icons and radars, when download_images is true
weapons/weapons_raw_values.csv
weapons/weapons_simple.csv
_manifest/<ingest_run_id>.json
_manifest/latest.json
```

`ingest_run_id` is a UTC timestamp. The download task passes it to later tasks. A manual notebook run falls back to `_manifest/latest.json` or to the latest id already in bronze.

### 1. `batch/download_reference_data.py`

Does not create a table. Downloads the two sources, writes the files above, and records each file in the manifest (URL, volume path, size, SHA-256).

### 2. `batch/bronze_reference_data.py`

Appends the current snapshot. File contents are not rewritten: text goes into a string column, images are metadata only.

| Table | Grain | Data |
| --- | --- | --- |
| `bronze.map_icons_raw` | Text file | `available.json` and each radar overview file, plus source commit and `raw_content` |
| `bronze.weapon_spreadsheet_raw` | Spreadsheet line | Raw Values and Simple sheets, with `line_number` and `raw_line` |
| `bronze.map_assets_raw` | Image | Path, file name, size, hash, and whether the file is a radar or an icon |

### 3. `batch/silver_reference_data.py`

Overwrites four tables from the latest `ingest_run_id`. `expect_all_or_drop` removes rows that fail a rule and prints the counts. A null condition counts as a failure, same as a Lakeflow expectation.

| Table | Grain | Data |
| --- | --- | --- |
| `silver.map_reference` | Map | Display name, family (`de` bomb defusal, `cs` hostage, …), radar transform (`pos`, `scale`, `rotate`, `zoom`), image URLs, insets, plant and spawn fractions |
| `silver.map_level` | Map and radar level | Altitude range and the matching radar image. Single-level maps get a synthetic `default` level |
| `silver.map_zone` | Map and zone | `bombsite_a`, `bombsite_b`, `ct_spawn`, `t_spawn` as fractions of the radar image |
| `silver.weapon_attributes` | Weapon | Damage, price, kill award, cycle time, penetration, spread, recoil, zoom. `gsi_weapon_name` adds the `weapon_` prefix |

Overview files are Valve KeyValues. Community maps generated by RadGen often have no plant zones. Hostage and arms-race maps have spawns only.

### 4. `batch/gold_reference_data.py`

Overwrites the reference dimensions. Radar images are 1024×1024. World Y is flipped against pixel Y because the radar counts from the top edge.

| Table | Grain | Data |
| --- | --- | --- |
| `gold.dim_map_reference` | Map | Radar parameters, world bounds of the radar square, icon path, level count |
| `gold.dim_map_level` | Map and radar level | `altitude_min` inclusive, `altitude_max` exclusive, so a shared border stays on one level |
| `gold.dim_map_zone` | Map and zone | The same zones converted to world coordinates |
| `gold.dim_weapon_spec` | Weapon | Silver attributes plus `fire_rate_rpm`, `damage_per_second`, and `kills_with_one_headshot` |

`kills_with_one_headshot` ignores helmet, armor, and range falloff.

### 5. `batch/validate_reference_data.py`

Reads the gold reference tables and fails the job when a check fails. It does not write a table.

Checks: unique `map_key` and `weapon_key`, unique level and zone grains, every radar map has a level, levels and zones belong to a map, and `fire_rate_rpm` is between 0 and 2000. Price, damage, and radar-scale rules already run in silver.

### 6. `batch/telemetry_coverage.py`

Not part of the job. Run it by hand after the GSI pipeline has data. It compares `gold.dim_map` and `gold.dim_weapon` with the reference dimensions and prints how many telemetry keys have a match. Knives, grenades, C4, and the taser are expected to miss.