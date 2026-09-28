"""Match and round boundaries in a GSI packet stream."""

from __future__ import annotations

import uuid
from typing import Any

from . import filters
from .envelope import (
    EVENT_MATCH_END,
    EVENT_MATCH_START,
    EVENT_ROUND_END,
    EVENT_ROUND_START,
    EVENT_TICK,
    GsiEvent,
    build_map_block,
    build_player_snapshot,
    build_tick_body,
)

Payload = dict[str, Any]

# Stable namespace so a replay of the same packets gets the same match_id.
MATCH_NAMESPACE = uuid.UUID("6f0a1d2e-7c35-5a4b-9f18-0c2d3e4b5a6f")


class MatchTracker:
    """Turns a GSI packet stream into match lifecycle events and ticks.

    The tracker sees every packet, not only 'ours'.
    """

    def __init__(self) -> None:
        self.match_id: str | None = None
        self.match_ended = True
        self.map_name: str | None = None
        self.steamid: str | None = None
        self.match_start_ts: int | None = None
        self.last_ts: int | None = None
        self.last_map_round: int | None = None
        self.last_map_block: dict[str, Any] = {}
        self.started_rounds: set[int] = set()
        self.ended_rounds: set[int] = set()
        self.current_round: int | None = None
        self.last_round_wins: dict[str, str] = {}
        self._own_snapshot: dict[str, Any] | None = None
        self._own_snapshot_round: int | None = None

    def process(self, payload: Payload) -> list[GsiEvent]:
        events: list[GsiEvent] = []
        provider = payload.get("provider") or {}
        steamid = provider.get("steamid")
        provider_ts = provider.get("timestamp")

        if not filters.is_in_match(payload):
            events.extend(self._close_match(provider_ts))
            self.last_ts = provider_ts
            return events

        map_block = payload["map"]
        if map_block.get("round_wins"):
            self.last_round_wins = map_block["round_wins"]

        if map_block.get("phase") == "gameover":
            self.last_ts = provider_ts
            if filters.is_own_player(payload):
                self._remember_own_state(payload, self.current_round)
                events.extend(self._close_match(provider_ts))
            return events

        if self._is_new_match(map_block):
            events.extend(self._close_match(self.last_ts))
            events.append(self._open_match(payload, steamid, provider_ts))

        self.steamid = steamid
        self.last_ts = provider_ts
        self.last_map_round = map_block.get("round")
        self.last_map_block = build_map_block(payload)

        phase = filters.round_phase(payload)
        round_number = self._round_number(map_block.get("round"), phase)
        if round_number is not None:
            self.current_round = round_number

        if filters.is_own_player(payload):
            self._remember_own_state(payload, round_number)

        if (
            round_number is not None
            and round_number not in self.started_rounds
            and phase in ("freezetime", "live")
        ):
            self.started_rounds.add(round_number)
            events.append(self._round_start(payload, round_number, provider_ts))

        if round_number is not None and phase == "over" and round_number not in self.ended_rounds:
            self.ended_rounds.add(round_number)
            events.append(self._round_end(payload, round_number, provider_ts))

        if filters.is_live_tick(payload):
            events.append(
                GsiEvent(
                    event_type=EVENT_TICK,
                    match_id=self.match_id or "",
                    steamid=steamid,
                    round_number=round_number,
                    provider_ts=provider_ts,
                    body=build_tick_body(payload),
                )
            )

        return events

    def finalize(self) -> list[GsiEvent]:
        """Closes the open match when the server shuts down."""
        return self._close_match(self.last_ts)

    @staticmethod
    def _round_number(map_round: int | None, phase: str | None) -> int | None:
        if map_round is None:
            return None
        if phase == "over":
            return max(map_round - 1, 0)
        return map_round

    def _is_new_match(self, map_block: dict[str, Any]) -> bool:
        if self.match_id is None or self.match_ended:
            return True
        if map_block.get("name") != self.map_name:
            return True
        map_round = map_block.get("round")
        if map_round is not None and self.last_map_round is not None and map_round < self.last_map_round:
            return True
        return False

    def _open_match(self, payload: Payload, steamid: str | None, provider_ts: int | None) -> GsiEvent:
        map_block = payload["map"]
        self.map_name = map_block.get("name")
        self.match_start_ts = provider_ts
        self.match_id = str(uuid.uuid5(MATCH_NAMESPACE, f"{steamid}|{self.map_name}|{provider_ts}"))
        self.match_ended = False
        self.started_rounds.clear()
        self.ended_rounds.clear()
        self.current_round = None
        self.last_round_wins = {}
        self._own_snapshot = None
        self._own_snapshot_round = None
        return GsiEvent(
            event_type=EVENT_MATCH_START,
            match_id=self.match_id,
            steamid=steamid,
            round_number=None,
            provider_ts=provider_ts,
            body={"map": build_map_block(payload)},
        )

    def _close_match(self, provider_ts: int | None) -> list[GsiEvent]:
        if self.match_id is None or self.match_ended:
            return []
        events = self._finish_pending_round(provider_ts)
        self.match_ended = True
        events.append(
            GsiEvent(
                event_type=EVENT_MATCH_END,
                match_id=self.match_id,
                steamid=self.steamid,
                round_number=None,
                provider_ts=provider_ts,
                body={
                    "map": self.last_map_block,
                    "rounds_played": len(self.ended_rounds),
                    "started_at_provider_ts": self.match_start_ts,
                },
            )
        )
        return events

    def _finish_pending_round(self, provider_ts: int | None) -> list[GsiEvent]:
        """Closes the last round, which never gets an `over` phase."""
        round_number = self.current_round
        if round_number is None or round_number in self.ended_rounds:
            return []
        win_reason = self.last_round_wins.get(str(round_number + 1))
        if not win_reason:
            return []
        self.ended_rounds.add(round_number)
        win_team = "CT" if win_reason.startswith("ct_") else "T" if win_reason.startswith("t_") else None
        return [
            self._build_round_end(
                round_number, provider_ts, self.last_map_block, win_team, win_reason
            )
        ]

    def _remember_own_state(self, payload: Payload, round_number: int | None) -> None:
        player = payload.get("player") or {}
        if "state" not in player:
            return
        self._own_snapshot = build_player_snapshot(payload)
        self._own_snapshot_round = round_number

    def _round_start(self, payload: Payload, round_number: int, provider_ts: int | None) -> GsiEvent:
        body: dict[str, Any] = {
            "map": build_map_block(payload),
            "phase": filters.round_phase(payload),
        }
        if filters.is_own_player(payload):
            body["player"] = build_player_snapshot(payload)
        return GsiEvent(
            event_type=EVENT_ROUND_START,
            match_id=self.match_id or "",
            steamid=self.steamid,
            round_number=round_number,
            provider_ts=provider_ts,
            body=body,
        )

    def _round_end(self, payload: Payload, round_number: int, provider_ts: int | None) -> GsiEvent:
        round_block = payload.get("round") or {}
        round_wins = (payload.get("map") or {}).get("round_wins") or self.last_round_wins
        return self._build_round_end(
            round_number,
            provider_ts,
            build_map_block(payload),
            round_block.get("win_team"),
            round_wins.get(str(round_number + 1)),
        )

    def _build_round_end(
        self,
        round_number: int,
        provider_ts: int | None,
        map_block: dict[str, Any],
        win_team: str | None,
        win_reason: str | None,
    ) -> GsiEvent:
        body: dict[str, Any] = {
            "map": map_block,
            "win_team": win_team,
            "win_reason": win_reason,
        }

        if self._own_snapshot is not None and self._own_snapshot_round == round_number:
            snapshot = self._own_snapshot
            body["player"] = snapshot
            health = (snapshot.get("state") or {}).get("health")
            body["player_survived"] = health is not None and health > 0
        return GsiEvent(
            event_type=EVENT_ROUND_END,
            match_id=self.match_id or "",
            steamid=self.steamid,
            round_number=round_number,
            provider_ts=provider_ts,
            body=body,
        )
