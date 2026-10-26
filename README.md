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
| [`infra/`](infra/) | Terraform: PROD workspace, storage, Unity Catalog, identities; the CI catalog in DEV | |
| [`.github/workflows/`](.github/workflows/) | CI on pull requests, CD to PROD, Terraform, nightly drift check | [CI/CD](#cicd) |

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

`dev` is for deploys from your machine. `ci` and `prod` are deployed by GitHub Actions. Start the local server from [`python/`](python/) after copying the GSI config into the game cfg directory.

## Tests

```bash
pip install -r requirements-dev.txt
pytest          # Spark tests skip when PySpark is not installed
ruff check .
```

## CI/CD

GitHub Actions signs in to Azure through OIDC as user-assigned managed identities. The repository stores no secrets.

| Workflow | Trigger | Identity | What it does |
| --- | --- | --- | --- |
| [`ci.yml`](.github/workflows/ci.yml) | Pull request to `main` | `id-cs2-ci` | Ruff and pytest, including Spark tests. Then validates `dev` and `ci`, deploys `ci` to the DEV workspace, checks the pipeline graph, and runs the reference job |
| [`cd.yml`](.github/workflows/cd.yml) | Push to `main`, manual | `id-cs2-prod-deployer`, environment `prod` | Validates, plans, and deploys `prod`, checks the pipeline graph, deploys the app source, then stops the app |
| [`infra.yml`](.github/workflows/infra.yml) | Changes in `infra/azure` or `infra/databricks` | `id-cs2-terraform` | Plans both stacks on a pull request. Applies `azure`, then `databricks`, on merge, in environment `infra` |
| [`drift.yml`](.github/workflows/drift.yml) | Nightly, manual | deployer and `id-cs2-terraform` | Fails if the bundle or Terraform plan for PROD is not empty |

Pull requests never load the `prod` target. Only the deployer can, because an app must be deployed by its `run_as` identity, and only jobs in the `prod` environment can sign in as it.

A deploy that deletes or recreates a resource fails at the confirmation prompt. Read the plan in the run summary, then run CD manually with `approve_destructive`. The manual run can also load the reference data and refresh the pipeline. Deploys never load data.

The app is stopped after each deploy because a running app is billed by the hour. Start it with `databricks apps start cs2-tracker -t prod`, or set `PROD_APP_KEEP_RUNNING` to `true` in the `prod` environment.

| Variable | Scope | Value |
| --- | --- | --- |
| `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID` | Repository | Tenant and subscription |
| `TF_CLIENT_ID` | Repository | `terraform -chdir=infra/azure output -raw terraform_client_id` |
| `DEV_CI_CLIENT_ID` | Repository | `terraform -chdir=infra/dev output -raw ci_client_id` |
| `AZURE_CLIENT_ID` | Environment `prod` | `terraform -chdir=infra/azure output -raw deployer_client_id` |
| `PROD_APP_KEEP_RUNNING` | Environment `prod`, optional | `true` keeps the app running after a deploy |

[`infra/bootstrap/bootstrap.ps1`](infra/bootstrap/bootstrap.ps1) creates the state storage and the Terraform identity. `infra/dev` and the bootstrap run from your machine, because the Terraform identity has no access to DEV.
