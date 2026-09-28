# CS2 Tracker

Counter-Strike 2 match telemetry and static game reference data, in one Unity Catalog.

A local server receives Game State Integration packets from the game and sends normalized events to Azure Event Hubs. A Databricks pipeline turns that stream into a star schema. A separate job loads map and weapon reference data. Facts join the reference dimensions on `map_key` and `weapon_key`. The two workloads do not share table ownership.

| Path | What it is | Detail |
| --- | --- | --- |
| [`python/`](python/) | Local GSI server | [python/README.md](python/README.md) |
| [`databricks/`](databricks/) | Lakeflow pipeline and reference-data job | [databricks/README.md](databricks/README.md) |
| [`databricks.yml`](databricks.yml) | Asset bundle. Dev catalog `cs2_dev`, prod catalog `cs2_prod` | |

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

Create the catalog schemas and the `bronze.raw` volume once with [`databricks/01_architecture_setup.ipynb`](databricks/01_architecture_setup.ipynb).

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      cs2_medallion -t dev
databricks bundle run      cs2_reference_data -t dev
```

The prod target host in [`databricks.yml`](databricks.yml) is a placeholder. Replace it before the first prod deploy. Start the local server from [`python/`](python/) after copying the GSI config into the game cfg directory.
