"""Konfiguracja lokalnego serwera GSI (wartosci nadpisywalne zmiennymi srodowiska)."""

import os

# Azure
KV_URI = os.getenv("CS2_KV_URI", "https://kv-cs2tracker.vault.azure.net/")
EVENT_HUB_NAME = os.getenv("CS2_EVENT_HUB_NAME", "cs2tracker_evh")
EVH_CONN_STRING_SECRET = os.getenv("CS2_EVH_SECRET_NAME", "evh-connection-string")

# Serwer HTTP nasluchujacy na pakiety z gry
HOST = os.getenv("CS2_GSI_HOST", "127.0.0.1")
PORT = int(os.getenv("CS2_GSI_PORT", "3000"))
LOG_LEVEL = os.getenv("CS2_LOG_LEVEL", "INFO")

# Ujscie zdarzen: wylaczenie kieruje ruch do lokalnego pliku JSONL
SEND_TO_EVENTHUB = os.getenv("CS2_SEND_TO_EVENTHUB", "true").lower() not in ("0", "false", "no")
OFFLINE_OUTPUT_PATH = os.getenv("CS2_OFFLINE_OUTPUT", "output/events.jsonl")

# Bufor i wysylka
MAX_BATCH_EVENTS = int(os.getenv("CS2_MAX_BATCH_EVENTS", "100"))
FLUSH_INTERVAL_S = float(os.getenv("CS2_FLUSH_INTERVAL_S", "0.2"))
QUEUE_MAXSIZE = int(os.getenv("CS2_QUEUE_MAXSIZE", "10000"))
MAX_RETRIES = int(os.getenv("CS2_MAX_RETRIES", "3"))
SPOOL_PATH = os.getenv("CS2_SPOOL_PATH", "spool/pending_events.jsonl")

# Po tylu sekundach ciszy kolejne pakiety traktujemy jako nowy mecz
MATCH_IDLE_TIMEOUT_S = int(os.getenv("CS2_MATCH_IDLE_TIMEOUT_S", "300"))
