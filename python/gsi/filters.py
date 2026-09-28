"""Filters for packets that describe our player in an active round."""

from typing import Any

Payload = dict[str, Any]


def provider_steamid(payload: Payload) -> str | None:
    return (payload.get("provider") or {}).get("steamid")


def player_steamid(payload: Payload) -> str | None:
    return (payload.get("player") or {}).get("steamid")


def is_own_player(payload: Payload) -> bool:
    """Whether `player` is the client owner."""
    steamid = player_steamid(payload)
    return steamid is not None and steamid == provider_steamid(payload)


def map_phase(payload: Payload) -> str | None:
    return (payload.get("map") or {}).get("phase")


def round_phase(payload: Payload) -> str | None:
    return (payload.get("round") or {}).get("phase")


def is_in_match(payload: Payload) -> bool:
    return payload.get("map") is not None


def is_live_tick(payload: Payload) -> bool:
    """Active round only."""
    return (
        is_own_player(payload)
        and map_phase(payload) == "live"
        and round_phase(payload) == "live"
    )
