"""Filtry decydujace, ktore pakiety GSI opisuja nas i aktywna rozgrywke."""

from typing import Any

Payload = dict[str, Any]


def provider_steamid(payload: Payload) -> str | None:
    return (payload.get("provider") or {}).get("steamid")


def player_steamid(payload: Payload) -> str | None:
    return (payload.get("player") or {}).get("steamid")


def is_own_player(payload: Payload) -> bool:
    """Czy blok ``player`` opisuje wlasciciela klienta gry.

    Po smierci gra przelacza blok ``player`` na obserwowanego gracza: albo na
    kamere swobodna (wtedy zostaje samo ``spectarget`` z id encji, bez
    ``steamid``), albo na kompletny blok innego gracza, ktorego nic nie
    odroznia od naszego poza wartoscia ``steamid``.
    """
    steamid = player_steamid(payload)
    return steamid is not None and steamid == provider_steamid(payload)


def map_phase(payload: Payload) -> str | None:
    return (payload.get("map") or {}).get("phase")


def round_phase(payload: Payload) -> str | None:
    return (payload.get("round") or {}).get("phase")


def is_in_match(payload: Payload) -> bool:
    """Czy gracz jest w meczu, a nie w menu lub na ekranie ladowania."""
    return payload.get("map") is not None


def is_live_tick(payload: Payload) -> bool:
    """Czy pakiet nadaje sie na tick wysokiej czestotliwosci.

    Przepuszczamy wylacznie aktywna faze rundy: bez menu, warmupu, freezetime,
    przerwy miedzy polowami i ekranu koncowego.
    """
    return (
        is_own_player(payload)
        and map_phase(payload) == "live"
        and round_phase(payload) == "live"
    )
