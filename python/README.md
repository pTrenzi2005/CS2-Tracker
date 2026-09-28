# GSI client

A local server receives Counter-Strike 2 telemetry, keeps our player's active round, and sends normalized events to Azure Event Hubs.

# Layout

```text
python/
  gsi_server.py              FastAPI app
  config.py                  Settings, each overridable by an environment variable
  gsi_config_template.txt    File the game reads from its cfg directory
  requirements.txt
  gsi/
    filters.py               Own-player and live-round checks
    match_state.py           Match and round boundaries
    envelope.py              Event envelope and payload normalization
    sender.py                Queue, batches, and Event Hubs retries
```

# Setup

```powershell
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

Copy [gsi_config_template.txt](gsi_config_template.txt) into the game folder as `...\Counter-Strike Global Offensive\game\csgo\cfg\gamestate_integration_cs2tracker.cfg`, retry the game then start the server:

```powershell
venv\Scripts\python gsi_server.py
```

The server listens on `http://127.0.0.1:3000`. `GET /health` returns the current `match_id`, map, completed rounds, event counters, sender stats, and queue length.

The Event Hubs connection string comes from Azure Key Vault (`CS2_KV_URI`, secret `evh-connection-string`) through `DefaultAzureCredential`. Stop the server with Ctrl+C. Shutdown closes the open match with `match_end` and flushes the queue.

| Variable | Default | Role |
| --- | --- | --- |
| `CS2_KV_URI` | `https://kv-cs2tracker.vault.azure.net/` | Key Vault that holds the connection string |
| `CS2_EVENT_HUB_NAME` | `cs2tracker_evh` | Event Hub name |
| `CS2_EVH_SECRET_NAME` | `evh-connection-string` | Secret name inside the vault |
| `CS2_GSI_HOST` | `127.0.0.1` | Bind address. Must match the `uri` in the cfg file |
| `CS2_GSI_PORT` | `3000` | Bind port |
| `CS2_LOG_LEVEL` | `INFO` | Python log level |
| `CS2_MAX_BATCH_EVENTS` | `100` | Events per flush |
| `CS2_FLUSH_INTERVAL_S` | `0.2` | Flush at least this often |
| `CS2_QUEUE_MAXSIZE` | `10000` | Queue cap. Further events are dropped |
| `CS2_MAX_RETRIES` | `3` | Send attempts before a batch is dropped |

# Events

Every event uses the same envelope: `schema_version`, `event_type`, `event_id`, `session_id`, `match_id`, `seq`, `client_ts`, `provider_ts`, `steamid`, `round_number`, `payload`.

| Event | When | Payload |
| --- | --- | --- |
| `tick` | `map.phase` and `round.phase` are `live`, and `player.steamid` equals `provider.steamid` | Map, round, player state, weapons, position, bomb |
| `match_start` | A new match opens | Map block |
| `round_start` | First `freezetime` or `live` packet of a round | Map, phase, and our player when the packet is ours |
| `round_end` | Round phase `over`, or the last round closed from `map.round_wins` | `win_team`, `win_reason`, and the last known state of our player in that round |
| `match_end` | Menu, scoreboard once our own state is visible, or server shutdown | Map, rounds played, start timestamp |

Menu, warmup, freeze time, halftime, and the scoreboard do not become ticks.

A new match starts when no match is open, the map name changes, or the round counter goes backwards. `match_id` is `uuid5` of steamid, map name, and the provider timestamp, so a replay of the same packets gets the same id.

# Modules

### 1. `gsi_server.py`

Reads each POST, asks `MatchTracker` for events, and enqueues them. A body that is not JSON is counted as `invalid` and ignored. A processing error is counted and logged. It does not stop the game.

### 2. `gsi/filters.py`

`is_own_player` compares `player.steamid` with `provider.steamid`. `is_in_match` is true when a `map` block exists. `is_live_tick` requires our player and both phases equal to `live`.

### 3. `gsi/match_state.py`

Watches every packet, not only ours. `map` and `round` do not depend on who the camera shows, and a round can end with no `over` packet for our player. Filtering on steamid first would drop that winner.

The last round never gets an `over` phase. The game goes straight to the scoreboard, and the winner is taken from `map.round_wins`. On `gameover` the match closes only after a packet that still describes our player, so the death on that boundary is kept. `finalize()` closes whatever is still open at shutdown.

### 4. `gsi/envelope.py`

Builds the envelope and trims the payload to a stable shape. Weapons arrive as `weapon_0`..`weapon_N` and are stored as an array, so Spark sees one schema. Nicknames are HTML-unescaped. `provider.timestamp` is one-second resolution and can cover several packets, so order comes from `seq` and millisecond `client_ts`.

### 5. `gsi/sender.py`

Batches by `match_id`, which is also the Event Hubs partition key, so one match stays ordered on one partition. A full queue drops the new event. A batch that still fails after the retry limit is counted as lost.

# GSI behavior this code accounts for

After death, `player` describes whoever is being watched. That is either a free camera (`spectarget` and no `steamid`) or another player's full block. Only `player.steamid == provider.steamid` tells the two apart.

`map.round` counts finished rounds. During round N it is N, but the `over` packet has already moved it to N+1, so the round number is stepped back by one in that phase. `round_wins` is 1-based. Our round numbers are 0-based.
