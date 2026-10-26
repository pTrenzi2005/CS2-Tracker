# CS2 Tracker

Counter-Strike 2 match telemetry and static game reference data, in one Unity Catalog.

A local server receives Game State Integration packets from the game and sends normalized events to Azure Event Hubs. A Databricks pipeline turns that stream into a star schema. A separate job loads map and weapon reference data. Facts join the reference dimensions on `map_key` and `weapon_key`. The two workloads do not share table ownership. A Databricks App starts both and shows the match dashboards.

| Path | What it is | Detail |
| --- | --- | --- |
| [`python/`](python/) | Local GSI server | [python/README.md](python/README.md) |
| [`databricks/`](databricks/) | Lakeflow pipeline and reference-data job | [databricks/README.md](databricks/README.md) |
| [`dashboards/`](dashboards/) | Databricks App: run buttons and match dashboards | [dashboards/README.md](dashboards/README.md) |
| [`databricks.yml`](databricks.yml) | Asset bundle. Targets `dev` (`cs2_dev`), `ci` (`cs2_ci`), `prod` (`cs2_prod`) | |
| [`tests/`](tests/) | pytest: GSI client, pipeline and reference transforms on local Spark | |

## Data

**Match telemetry.** The game posts JSON to `http://127.0.0.1:3000`. The server keeps our player's active round and emits `tick`, `match_start`, `round_start`, `round_end`, and `match_end`. GSI sends state, not kill or damage events. Those are derived later from counter changes between ticks. The Event Hubs connection string comes from Azure Key Vault.

**Reference data.** [MurkyYT/cs2-map-icons](https://github.com/MurkyYT/cs2-map-icons) supplies radar transforms, altitude levels, plant zones, and images. The [Kaggle weapon spreadsheet](https://www.kaggle.com/datasets/billpureskillgg/cs2-weapon-spreadsheet) supplies weapon stats. The job downloads both into the `bronze.raw` volume.

## Medallion

Both workloads use `bronze`, `silver`, and `gold` in the same catalog.

| Layer | Telemetry pipeline | Reference job |
| --- | --- | --- |
| Bronze | Raw Kafka payload, unchanged | Append-only snapshot of downloaded files, keyed by `ingest_run_id` |
| Silver | Parsed events, ticks, round boundaries, and derived kills, deaths, and damage | One row per map, radar level, zone, and weapon. Latest snapshot, overwritten |
| Gold | Star schema: player, map, weapon, round, and match dimensions, plus facts and a match summary | Reference dimensions only: radar bounds, levels, zones, and weapon specs |

The pipeline is triggered, not continuous. The reference job has no schedule. Run it again after a game update. Reference tables are written by the job, so they survive a pipeline rebuild.

Raw SteamID64 stays in silver. Gold stores `player_key` (`sha2` of the id) and a masked id. Pipeline secrets live in the Databricks secret scope `cs2tracker-secrets`, not in source.

## Run

The bundle deploys everything below the catalog: schemas, the `bronze.raw` volume, grants, the pipeline, jobs, the warehouse, and the app. The catalog and the external locations for the `bronze`, `silver`, and `gold` containers must exist first.

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      cs2_medallion -t dev
databricks bundle run      cs2_reference_data -t dev
databricks bundle run      cs2_tracker -t dev
```

The last command starts the app and prints its URL. The app reads gold through a schema grant, so one deploy works on an empty catalog. `databricks bundle plan -t dev` after a deploy should report no changes. Anything else is drift.

Prod values marked `PROD_` in [`databricks.yml`](databricks.yml) are placeholders until the prod infrastructure exists. Start the local server from [`python/`](python/) after copying the GSI config into the game cfg directory.

## Tests

```bash
pip install -r requirements-dev.txt
pytest          # Spark tests skip when PySpark is not installed
ruff check .
```
