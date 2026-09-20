"""Lokalny serwer Game State Integration: filtruje telemetrie CS2 i wysyla ja do Event Hubs."""

import logging
from collections import defaultdict
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request

import config
from gsi.envelope import EnvelopeFactory
from gsi.match_state import MatchTracker
from gsi.sender import EventHubSender, EventSink, FileSink

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("gsi")


def build_sink() -> EventSink:
    if not config.SEND_TO_EVENTHUB:
        return FileSink(config.OFFLINE_OUTPUT_PATH)

    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=config.KV_URI, credential=credential)
    conn_string = secret_client.get_secret(config.EVH_CONN_STRING_SECRET).value
    return EventHubSender(
        conn_string=conn_string,
        eventhub_name=config.EVENT_HUB_NAME,
        max_batch_events=config.MAX_BATCH_EVENTS,
        flush_interval_s=config.FLUSH_INTERVAL_S,
        queue_maxsize=config.QUEUE_MAXSIZE,
        max_retries=config.MAX_RETRIES,
        spool_path=config.SPOOL_PATH,
    )


tracker = MatchTracker(idle_timeout_s=config.MATCH_IDLE_TIMEOUT_S)
envelopes = EnvelopeFactory()
sink: EventSink = build_sink()
counters: dict[str, int] = defaultdict(int)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await sink.start()
    log.info("Serwer GSI nasluchuje na http://%s:%s", config.HOST, config.PORT)
    yield
    for event in tracker.finalize():
        sink.submit(envelopes.wrap(event))
    await sink.stop()
    log.info("Podsumowanie: %s", dict(counters))


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def gsi_endpoint(request: Request):
    # Handler musi wrocic natychmiast: GSI czeka na odpowiedz blokujac watek gry.
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001 - heartbeat bez ciala albo uszkodzony JSON
        counters["invalid"] += 1
        return {"status": "ignored"}

    counters["received"] += 1
    try:
        for event in tracker.process(payload):
            counters[event.event_type] += 1
            sink.submit(envelopes.wrap(event))
    except Exception:  # noqa: BLE001 - blad przetwarzania nie moze zatrzymac gry
        counters["errors"] += 1
        log.exception("Nie udalo sie przetworzyc pakietu GSI")

    return {"status": "ok"}


@app.get("/health")
async def health():
    return {
        "session_id": envelopes.session_id,
        "match_id": tracker.match_id,
        "map": tracker.map_name,
        "rounds_completed": len(tracker.ended_rounds),
        "counters": dict(counters),
        "sink": dict(sink.stats),
        "queue_size": getattr(sink, "queue_size", 0),
    }


if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT, log_level="warning")
