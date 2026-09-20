"""Normalizacja payloadu i koperta transportowa."""

from gsi.envelope import (
    EnvelopeFactory,
    GsiEvent,
    active_weapon_name,
    build_player_snapshot,
    build_tick_body,
    normalize_weapons,
)

OWN_STEAMID = "76561198442208095"


def test_bronie_zamienione_na_tablice_o_stalym_schemacie(packet):
    weapons = normalize_weapons(packet(115)["player"]["weapons"])
    assert [weapon["slot"] for weapon in weapons] == [0, 1, 2, 3, 4, 5, 6, 7]
    assert weapons[4]["name"] == "weapon_hegrenade"
    assert active_weapon_name(weapons) == "weapon_hegrenade"
    assert all("paintkit" not in weapon for weapon in weapons)


def test_pusty_slownik_broni_daje_pusta_tablice():
    assert normalize_weapons({}) == []
    assert normalize_weapons(None) == []


def test_snapshot_pomija_pola_nieanalityczne(packet):
    snapshot = build_player_snapshot(packet(50))
    assert "spectarget" not in snapshot
    assert "steamid" not in snapshot
    assert snapshot["position"] == "2272.00, 672.00, 149.14"


def test_nick_bez_encji_html(packet):
    assert build_player_snapshot(packet(50))["name"] == "I'm Retard123 elomelo320"


def test_tick_nie_przenosi_bloku_previously(packet):
    body = build_tick_body(packet(115))
    assert "previously" not in body
    assert "added" not in body
    assert set(body) == {"map", "round", "player"}


def test_koperta_numeruje_zdarzenia_rosnaco():
    factory = EnvelopeFactory(session_id="test")
    event = GsiEvent("tick", "match-1", OWN_STEAMID, 3, 1000, {})
    envelopes = [factory.wrap(event) for _ in range(3)]
    assert [e["seq"] for e in envelopes] == [1, 2, 3]
    assert len({e["event_id"] for e in envelopes}) == 3
    assert all(e["session_id"] == "test" for e in envelopes)


def test_koperta_ma_znacznik_czasu_w_milisekundach():
    factory = EnvelopeFactory()
    envelope = factory.wrap(GsiEvent("tick", "match-1", OWN_STEAMID, 0, 1000, {}))
    # provider.timestamp ma rozdzielczosc sekundy, wiec porzadkuje nas client_ts.
    assert envelope["client_ts"].endswith("Z")
    assert len(envelope["client_ts"].split(".")[-1]) == 4
