"""Normalizacja pakietow GSI i koperta zdarzenia wysylanego do Event Hubs."""

from __future__ import annotations

import html
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import count
from typing import Any, Iterable

SCHEMA_VERSION = "1.0"

EVENT_TICK = "tick"
EVENT_MATCH_START = "match_start"
EVENT_ROUND_START = "round_start"
EVENT_ROUND_END = "round_end"
EVENT_MATCH_END = "match_end"

Payload = dict[str, Any]

MAP_FIELDS = ("name", "mode", "phase", "round", "num_matches_to_win_series")
TEAM_FIELDS = ("score", "consecutive_round_losses", "timeouts_remaining", "matches_won_this_series")
PLAYER_FIELDS = ("name", "team", "activity", "observer_slot")
STATE_FIELDS = (
    "health",
    "armor",
    "helmet",
    "flashed",
    "smoked",
    "burning",
    "money",
    "round_kills",
    "round_killhs",
    "round_totaldmg",
    "equip_value",
)
MATCH_STATS_FIELDS = ("kills", "assists", "deaths", "mvps", "score")
WEAPON_FIELDS = ("name", "type", "state", "ammo_clip", "ammo_clip_max", "ammo_reserve")
BOMB_FIELDS = ("state", "position", "player", "countdown")
COUNTDOWN_FIELDS = ("phase", "phase_ends_in")


@dataclass(frozen=True)
class GsiEvent:
    """Zdarzenie rozpoznane przez tracker, jeszcze bez koperty transportowej."""

    event_type: str
    match_id: str
    steamid: str | None
    round_number: int | None
    provider_ts: int | None
    body: dict[str, Any] = field(default_factory=dict)


def _pick(source: dict[str, Any] | None, keys: Iterable[str]) -> dict[str, Any]:
    if not source:
        return {}
    return {key: source[key] for key in keys if key in source}


def _weapon_slot(key: str) -> int:
    """``weapon_3`` -> ``3``; nieznane klucze ladują na koncu listy."""
    _, _, suffix = key.partition("_")
    return int(suffix) if suffix.isdigit() else 999


def normalize_weapons(weapons: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Zamienia slownik ``weapon_0..weapon_N`` na tablice o stalym schemacie.

    GSI zwraca bron jako obiekt o zmiennej liczbie kluczy, wiec kazdy pakiet
    mialby inny ksztalt kolumny w Sparku. Tablica rekordow daje jeden schemat
    niezaleznie od tego, ile broni gracz akurat trzyma.
    """
    if not weapons:
        return []
    normalized = []
    for key in sorted(weapons, key=_weapon_slot):
        weapon = weapons[key]
        if not isinstance(weapon, dict):
            continue
        normalized.append({"slot": _weapon_slot(key), **_pick(weapon, WEAPON_FIELDS)})
    return normalized


def active_weapon_name(weapons: list[dict[str, Any]]) -> str | None:
    for weapon in weapons:
        if weapon.get("state") == "active":
            return weapon.get("name")
    return None


def build_map_block(payload: Payload) -> dict[str, Any]:
    map_data = payload.get("map") or {}
    block = _pick(map_data, MAP_FIELDS)
    for team in ("team_ct", "team_t"):
        if team in map_data:
            block[team] = _pick(map_data[team], TEAM_FIELDS)
    return block


def build_player_snapshot(payload: Payload) -> dict[str, Any]:
    """Stan naszego gracza bez pol nieuzywanych w analityce.

    Pomijamy ``previously`` i ``added`` (delty, ktore i tak liczymy w Silver),
    ``spectarget`` (po smierci wskazuje kogos innego) oraz ``paintkit``.
    """
    player = payload.get("player") or {}
    weapons = normalize_weapons(player.get("weapons"))
    snapshot: dict[str, Any] = _pick(player, PLAYER_FIELDS)
    if "name" in snapshot:
        # GSI escapuje nick jako encje HTML (``I&apos;m`` zamiast ``I'm``).
        snapshot["name"] = html.unescape(snapshot["name"])
    snapshot["state"] = _pick(player.get("state"), STATE_FIELDS)
    snapshot["match_stats"] = _pick(player.get("match_stats"), MATCH_STATS_FIELDS)
    snapshot["weapons"] = weapons
    snapshot["active_weapon"] = active_weapon_name(weapons)
    for key in ("position", "forward"):
        if key in player:
            snapshot[key] = player[key]
    return snapshot


def build_tick_body(payload: Payload) -> dict[str, Any]:
    body: dict[str, Any] = {
        "map": build_map_block(payload),
        "round": _pick(payload.get("round"), ("phase", "win_team", "bomb")),
        "player": build_player_snapshot(payload),
    }
    bomb = _pick(payload.get("bomb"), BOMB_FIELDS)
    if bomb:
        body["bomb"] = bomb
    countdowns = _pick(payload.get("phase_countdowns"), COUNTDOWN_FIELDS)
    if countdowns:
        body["phase_countdowns"] = countdowns
    return body


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EnvelopeFactory:
    """Nadaje zdarzeniom identyfikatory, kolejnosc i znacznik czasu klienta.

    ``provider.timestamp`` z GSI ma rozdzielczosc jednej sekundy, a przy
    ustawionym throttle potrafi opisywac kilka pakietow naraz, wiec nie
    porzadkuje zdarzen. Robi to ``seq`` razem z ``client_ts`` w milisekundach.
    """

    def __init__(self, session_id: str | None = None) -> None:
        self.session_id = session_id or str(uuid.uuid4())
        self._seq = count(1)

    def wrap(self, event: GsiEvent, client_ts: str | None = None) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "event_type": event.event_type,
            "event_id": str(uuid.uuid4()),
            "session_id": self.session_id,
            "match_id": event.match_id,
            "seq": next(self._seq),
            "client_ts": client_ts or utc_now_iso(),
            "provider_ts": event.provider_ts,
            "steamid": event.steamid,
            "round_number": event.round_number,
            "payload": event.body,
        }
