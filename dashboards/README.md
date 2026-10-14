# Dashboards app

A Databricks App in Streamlit. The home page starts the reference batch and the match analysis. The analysis page reads the gold star schema from Unity Catalog and draws one match, current or historical.

# Layout

```text
dashboards/
  app.py              Entry point and page navigation
  app.yaml            Start command and environment, resolved from app resources
  requirements.txt
  data.py             SQL warehouse queries, table names, radar images, map thumbnails
  runs.py             Start and read job runs
  views/
    home.py           RUN BATCH, RUN ANALYSIS, latest match
    analysis.py       Single page: radar overlays, match overview, momentum and economy, round pacing, round log
```

The app is defined in [`../databricks/resources/cs2_tracker.app.yml`](../databricks/resources/cs2_tracker.app.yml) and deploys with the bundle. The GSI server is not part of it. It keeps running next to the game, see [`../python/`](../python/).

# Buttons

| Button | Job | What it does |
| --- | --- | --- |
| RUN BATCH | `cs2_reference_data` | Maps, radars, and weapon stats into the reference dimensions |
| RUN ANALYSIS | `cs2_medallion_refresh` | One incremental update of `cs2_gsi_medallion`: new Event Hubs messages through bronze, silver, and gold |

The pipeline takes no match id. It reads whatever arrived since its last update. When the analysis run succeeds, the app opens the match with the latest `ended_at` in `gold.dim_match`. When it fails, the home page shows the run state and stays.

Runs execute as the job owner. The app service principal only holds `CAN_MANAGE_RUN`. While a run is active, its button is disabled and the status refreshes every 5 seconds. A page reload during a run still redirects when it ends.

# Data access

Queries run on the `cs2-tracker-app` serverless warehouse. Each table the app reads is an app resource with `SELECT`, and `bronze.raw` is a resource with `READ_VOLUME`. Databricks grants `USE CATALOG` and `USE SCHEMA` with them. `app.yaml` maps every resource to an environment variable that holds the full table name, so the code never hardcodes the catalog.

| Variable | Resource | Value |
| --- | --- | --- |
| `DATABRICKS_WAREHOUSE_ID` | `sql-warehouse` | Warehouse id |
| `CS2_REFERENCE_JOB_ID`, `CS2_ANALYSIS_JOB_ID` | `reference-job`, `analysis-job` | Job ids |
| `CS2_RAW_VOLUME` | `raw-volume` | `/Volumes/<catalog>/bronze/raw` |
| `CS2_DIM_MATCH` … `CS2_DIM_WEAPON_SPEC` | One per gold table | `<catalog>.gold.<table>` |

Match ids, rounds, and sides are bound as named parameters, not formatted into SQL. Query results are cached for 10 minutes. A finished run clears the cache.

Radar images come from the volume copy that the reference job downloads (`gold.dim_map_level.radar_volume_path`). If that file is missing, the app falls back to the source URL. Map thumbnails in the analysis header have no volume copy and always load from `gold.dim_map_reference.thumb_urls`.

# Run locally

The same code runs on a laptop against the workspace. Authentication comes from a CLI profile. Table names fall back to `CS2_CATALOG`.

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
$env:DATABRICKS_CONFIG_PROFILE = "<profile>"
$env:DATABRICKS_WAREHOUSE_ID = "<warehouse id>"
$env:CS2_CATALOG = "cs2_dev"
$env:CS2_REFERENCE_JOB_ID = "<job id>"   # optional, enables RUN BATCH
$env:CS2_ANALYSIS_JOB_ID = "<job id>"    # optional, enables RUN ANALYSIS
.venv\Scripts\streamlit run app.py
```

Locally the queries and runs use your own identity, not the app service principal.
