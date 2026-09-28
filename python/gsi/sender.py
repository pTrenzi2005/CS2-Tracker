"""Event Hubs delivery off the HTTP request path."""

from __future__ import annotations
import asyncio
import json
import logging
from collections import defaultdict
from typing import Any, AsyncIterator, Iterable

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

log = logging.getLogger(__name__)

_SHUTDOWN = object()


class EventHubSender:
    def __init__(
        self,
        conn_string: str,
        eventhub_name: str,
        *,
        max_batch_events: int = 100,
        flush_interval_s: float = 0.2,
        queue_maxsize: int = 10_000,
        max_retries: int = 3,
    ) -> None:
        self._conn_string = conn_string
        self._eventhub_name = eventhub_name
        self.max_batch_events = max_batch_events
        self.flush_interval_s = flush_interval_s
        self.max_retries = max_retries
        self.stats: dict[str, int] = defaultdict(int)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._producer: EventHubProducerClient | None = None
        self._worker: asyncio.Task | None = None

    async def start(self) -> None:
        self._producer = EventHubProducerClient.from_connection_string(
            conn_str=self._conn_string, eventhub_name=self._eventhub_name
        )
        self._worker = asyncio.create_task(self._run(), name="eventhub-sender")
        log.info("Event Hubs sender started (hub=%s)", self._eventhub_name)

    def submit(self, envelope: dict[str, Any]) -> bool:
        try:
            self._queue.put_nowait(envelope)
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            log.warning("Queue full, dropped event %s", envelope.get("event_id"))
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
        log.info("Sender stopped: %s", dict(self.stats))

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
        if not events:
            return
        # Partition on match_id so one match stays ordered on one partition.
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for envelope in events:
            groups[envelope.get("match_id") or "unknown"].append(envelope)
        for partition_key, group in groups.items():
            await self._send_group(partition_key, group)

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
                    log.error("Event %s exceeds the batch size limit", envelope.get("event_id"))
                    continue
            current.append(envelope)
        if current:
            yield batch, current

    async def _send_with_retry(self, batch: Any, events: list[dict[str, Any]]) -> bool:
        assert self._producer is not None
        for attempt in range(1, self.max_retries + 1):
            try:
                await self._producer.send_batch(batch)
            except Exception as exc:
                log.warning("Batch send failed (attempt %s/%s): %s", attempt, self.max_retries, exc)
                await asyncio.sleep(min(0.5 * 2**attempt, 5.0))
            else:
                self.stats["sent"] += len(events)
                return True
        self.stats["lost"] += len(events)
        log.error("Dropped %s events after %s failed attempts", len(events), self.max_retries)
        return False
