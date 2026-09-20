"""Wysylka zdarzen do Azure Event Hubs poza sciezka zadania HTTP.

GSI ma ``timeout 5.0`` i czeka na odpowiedz blokujac watek silnika gry, wiec
wysylka synchroniczna w handlerze zamienia kazdy retry do Azure na zacinke w
grze. Handler tylko odklada zdarzenie do kolejki, a pakowaniem w batche,
ponawianiem i buforowaniem na dysku zajmuje sie zadanie w tle.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, AsyncIterator, Iterable

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

log = logging.getLogger(__name__)

_SHUTDOWN = object()


class EventSink:
    """Wspolny interfejs ujscia zdarzen (Event Hubs albo plik lokalny)."""

    def __init__(self) -> None:
        self.stats: dict[str, int] = defaultdict(int)

    async def start(self) -> None:  # pragma: no cover - trywialne
        return None

    def submit(self, envelope: dict[str, Any]) -> bool:
        raise NotImplementedError

    async def stop(self) -> None:  # pragma: no cover - trywialne
        return None


class EventHubSender(EventSink):
    def __init__(
        self,
        conn_string: str,
        eventhub_name: str,
        *,
        max_batch_events: int = 100,
        flush_interval_s: float = 0.2,
        queue_maxsize: int = 10_000,
        max_retries: int = 3,
        spool_path: str | Path = "spool/pending_events.jsonl",
        spool_retry_interval_s: float = 30.0,
    ) -> None:
        super().__init__()
        self._conn_string = conn_string
        self._eventhub_name = eventhub_name
        self.max_batch_events = max_batch_events
        self.flush_interval_s = flush_interval_s
        self.max_retries = max_retries
        self.spool_path = Path(spool_path)
        self.spool_retry_interval_s = spool_retry_interval_s
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._producer: EventHubProducerClient | None = None
        self._worker: asyncio.Task | None = None
        self._next_spool_retry = 0.0

    async def start(self) -> None:
        self._producer = EventHubProducerClient.from_connection_string(
            conn_str=self._conn_string, eventhub_name=self._eventhub_name
        )
        self._worker = asyncio.create_task(self._run(), name="eventhub-sender")
        log.info("Sender Event Hubs wystartowal (hub=%s)", self._eventhub_name)

    def submit(self, envelope: dict[str, Any]) -> bool:
        try:
            self._queue.put_nowait(envelope)
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            log.warning("Kolejka pelna, odrzucono zdarzenie %s", envelope.get("event_id"))
            return False
        self.stats["queued"] += 1
        return True

    async def stop(self) -> None:
        if self._worker is not None:
            await self._queue.put(_SHUTDOWN)
            await self._worker
            self._worker = None
        if self._producer is not None:
            await self._producer.close()
            self._producer = None
        log.info("Sender zatrzymany: %s", dict(self.stats))

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        pending: list[dict[str, Any]] = []
        deadline = loop.time() + self.flush_interval_s
        while True:
            try:
                item = await asyncio.wait_for(
                    self._queue.get(), timeout=max(0.0, deadline - loop.time())
                )
            except asyncio.TimeoutError:
                item = None
            if item is _SHUTDOWN:
                await self._flush(pending)
                return
            if item is not None:
                pending.append(item)
            if pending and (len(pending) >= self.max_batch_events or loop.time() >= deadline):
                await self._flush(pending)
                pending = []
            if not pending:
                deadline = loop.time() + self.flush_interval_s

    async def _flush(self, events: list[dict[str, Any]]) -> None:
        if events:
            # Klucz partycji = match_id, dzieki czemu ticki jednego meczu
            # trafiaja do jednej partycji i zachowuja kolejnosc.
            groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for envelope in events:
                groups[envelope.get("match_id") or "unknown"].append(envelope)
            for partition_key, group in groups.items():
                await self._send_group(partition_key, group)
        await self._retry_spool()

    async def _send_group(self, partition_key: str, events: list[dict[str, Any]]) -> None:
        async for batch, batched in self._batches(partition_key, events):
            await self._send_with_retry(batch, batched)

    async def _batches(
        self, partition_key: str, events: Iterable[dict[str, Any]]
    ) -> AsyncIterator[tuple[Any, list[dict[str, Any]]]]:
        assert self._producer is not None
        batch = await self._producer.create_batch(partition_key=partition_key)
        current: list[dict[str, Any]] = []
        for envelope in events:
            data = EventData(json.dumps(envelope, ensure_ascii=False))
            try:
                batch.add(data)
            except ValueError:
                if current:
                    yield batch, current
                batch = await self._producer.create_batch(partition_key=partition_key)
                current = []
                try:
                    batch.add(data)
                except ValueError:
                    self.stats["oversized"] += 1
                    log.error("Zdarzenie %s przekracza limit batcha", envelope.get("event_id"))
                    continue
            current.append(envelope)
        if current:
            yield batch, current

    async def _send_with_retry(self, batch: Any, events: list[dict[str, Any]]) -> bool:
        assert self._producer is not None
        for attempt in range(1, self.max_retries + 1):
            try:
                await self._producer.send_batch(batch)
            except Exception as exc:  # noqa: BLE001 - kazdy blad transportu konczy sie spoolem
                log.warning("Wysylka batcha nieudana (proba %s/%s): %s", attempt, self.max_retries, exc)
                await asyncio.sleep(min(0.5 * 2**attempt, 5.0))
            else:
                self.stats["sent"] += len(events)
                return True
        self._spool(events)
        return False

    def _spool(self, events: list[dict[str, Any]]) -> None:
        self.spool_path.parent.mkdir(parents=True, exist_ok=True)
        with self.spool_path.open("a", encoding="utf-8") as handle:
            for envelope in events:
                handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        self.stats["spooled"] += len(events)
        log.error("Zbuforowano %s zdarzen na dysku (%s)", len(events), self.spool_path)

    async def _retry_spool(self) -> None:
        loop = asyncio.get_running_loop()
        if loop.time() < self._next_spool_retry or not self.spool_path.exists():
            return
        self._next_spool_retry = loop.time() + self.spool_retry_interval_s
        lines = self.spool_path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return
        # Plik czyscimy od razu: nieudane ponowienie dopisze zdarzenia z powrotem.
        self.spool_path.unlink()
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for line in lines:
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError:
                self.stats["spool_corrupt"] += 1
                continue
            groups[envelope.get("match_id") or "unknown"].append(envelope)
        log.info("Ponawiam wysylke %s zbuforowanych zdarzen", sum(len(g) for g in groups.values()))
        for partition_key, group in groups.items():
            await self._send_group(partition_key, group)


class FileSink(EventSink):
    """Tryb offline: zdarzenia trafiaja do pliku JSONL zamiast do Event Hubs."""

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)

    async def start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")
        log.info("Tryb offline, zdarzenia zapisywane do %s", self.path)

    def submit(self, envelope: dict[str, Any]) -> bool:
        self._handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        # Flush przy kazdym zdarzeniu: tryb offline sluzy do podgladu na zywo,
        # a serwer zwykle konczy sie ubiciem procesu, nie czystym shutdownem.
        self._handle.flush()
        self.stats["sent"] += 1
        return True

    async def stop(self) -> None:
        self._handle.flush()
        self._handle.close()
