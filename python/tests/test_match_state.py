"""Granice meczu i rund wyznaczane przez MatchTracker."""

from gsi.envelope import EVENT_MATCH_END, EVENT_MATCH_START, EVENT_ROUND_END, EVENT_TICK
from gsi.match_state import MatchTracker

OWN_STEAMID = "76561198442208095"


def make_payload(
    *,
    ts: int = 1000,
    map_name: str = "de_inferno",
    map_phase: str = "live",
    map_round: int = 0,
    round_phase: str = "live",
    health: int = 100,
    round_wins: dict[str, str] | None = None,
    win_team: str | None = None,
):
    map_block = {"name": map_name, "mode": "competitive", "phase": map_phase, "round": map_round}
    if round_wins is not None:
        map_block["round_wins"] = round_wins
    round_block = {"phase": round_phase}
    if win_team is not None:
        round_block["win_team"] = win_team
    return {
        "provider": {"steamid": OWN_STEAMID, "timestamp": ts},
        "map": map_block,
        "round": round_block,
        "player": {
            "steamid": OWN_STEAMID,
            "name": "tester",
            "team": "CT",
            "activity": "playing",
            "state": {"health": health, "round_kills": 0, "round_totaldmg": 0},
            "match_stats": {"kills": 0, "deaths": 0},
            "weapons": {"weapon_0": {"name": "weapon_knife", "type": "Knife", "state": "active"}},
            "position": "1.00, 2.00, 3.00",
            "forward": "1.00, 0.00, 0.00",
        },
    }


def types(events):
    return [event.event_type for event in events]


def test_numer_rundy_przesuniety_w_fazie_over():
    # map.round liczy rundy zakonczone, wiec w pakiecie "over" jest juz o 1 za duzo.
    assert MatchTracker._round_number(1, "over") == 0
    assert MatchTracker._round_number(1, "live") == 1
    assert MatchTracker._round_number(0, "over") == 0
    assert MatchTracker._round_number(None, "live") is None


def test_menu_nie_generuje_zdarzen(packet):
    tracker = MatchTracker()
    assert tracker.process(packet(0)) == []


def test_pierwszy_pakiet_otwiera_mecz_i_runde():
    tracker = MatchTracker()
    events = tracker.process(make_payload())
    assert types(events) == [EVENT_MATCH_START, "round_start", EVENT_TICK]
    assert tracker.match_id is not None


def test_runda_konczy_sie_tylko_raz():
    tracker = MatchTracker()
    tracker.process(make_payload(map_round=0, round_phase="live"))
    first = tracker.process(make_payload(ts=1001, map_round=1, round_phase="over"))
    second = tracker.process(make_payload(ts=1002, map_round=1, round_phase="over"))
    assert types(first) == [EVENT_ROUND_END]
    assert first[0].round_number == 0
    assert second == []


def test_zmiana_mapy_otwiera_nowy_mecz():
    tracker = MatchTracker()
    tracker.process(make_payload(map_name="de_inferno"))
    first_match = tracker.match_id
    events = tracker.process(make_payload(ts=1001, map_name="de_mirage", map_round=0))
    assert EVENT_MATCH_END in types(events)
    assert EVENT_MATCH_START in types(events)
    assert tracker.match_id != first_match


def test_przerwa_dluzsza_niz_limit_otwiera_nowy_mecz():
    tracker = MatchTracker(idle_timeout_s=60)
    tracker.process(make_payload(ts=1000, map_round=3))
    first_match = tracker.match_id
    tracker.process(make_payload(ts=1200, map_round=3))
    assert tracker.match_id != first_match


def test_gameover_konczy_mecz_dokladnie_raz():
    tracker = MatchTracker()
    tracker.process(make_payload())
    first = tracker.process(make_payload(ts=1001, map_phase="gameover"))
    second = tracker.process(make_payload(ts=1002, map_phase="gameover"))
    assert types(first) == [EVENT_MATCH_END]
    assert second == []


def test_powod_wygranej_czytany_z_map_round_wins():
    # Komponent map_round_wins trafia do bloku map jako round_wins, a nie na
    # najwyzszy poziom pakietu.
    wins = {"1": "t_win_elimination"}
    tracker = MatchTracker()
    tracker.process(make_payload(map_round=0, round_wins=wins))
    events = tracker.process(
        make_payload(ts=1001, map_round=1, round_phase="over", win_team="T", round_wins=wins)
    )
    assert types(events) == [EVENT_ROUND_END]
    assert events[0].body["win_reason"] == "t_win_elimination"


def test_ostatnia_runda_domykana_na_ekranie_koncowym():
    # Po ostatniej rundzie gra idzie prosto do gameover z faza freezetime, wiec
    # zwyciezcy nie ma nigdzie poza round_wins, a smierc widac dopiero tam.
    wins = {"1": "ct_win_elimination"}
    tracker = MatchTracker()
    tracker.process(make_payload(map_round=0, round_wins=wins))
    events = tracker.process(
        make_payload(
            ts=1002,
            map_round=1,
            map_phase="gameover",
            round_phase="freezetime",
            round_wins=wins,
            health=0,
        )
    )
    assert types(events) == [EVENT_ROUND_END, EVENT_MATCH_END]
    koniec = events[0]
    assert koniec.round_number == 0
    assert koniec.body["win_team"] == "CT"
    assert koniec.body["win_reason"] == "ct_win_elimination"
    assert koniec.body["player_survived"] is False


def test_ekran_koncowy_bez_naszego_stanu_nie_zamyka_meczu():
    tracker = MatchTracker()
    tracker.process(make_payload())
    obcy = make_payload(ts=1001, map_phase="gameover", round_phase="freezetime")
    obcy["player"]["steamid"] = "76561190000000000"
    assert tracker.process(obcy) == []
    assert types(tracker.finalize()) == [EVENT_MATCH_END]


def test_finalize_domyka_trwajacy_mecz():
    tracker = MatchTracker()
    tracker.process(make_payload())
    assert types(tracker.finalize()) == [EVENT_MATCH_END]
    assert tracker.finalize() == []


def test_koniec_rundy_wykryty_mimo_obcych_pakietow(envelopes):
    # W rundzie 1 nagranych danych zaden pakiet z faza "over" nie dotyczy nas
    # (kamera byla juz na przeciwniku), a mimo to znamy zwyciezce rundy.
    round_ends = {e["round_number"]: e for e in envelopes if e["event_type"] == EVENT_ROUND_END}
    assert set(round_ends) == {0, 1, 2}
    assert round_ends[1]["payload"]["win_team"] == "T"


def test_koniec_rundy_zachowuje_smierc_z_konca_rundy(envelopes):
    round_ends = {e["round_number"]: e for e in envelopes if e["event_type"] == EVENT_ROUND_END}
    body = round_ends[0]["payload"]
    assert body["player_survived"] is False
    assert body["player"]["state"]["health"] == 0
    assert body["player"]["position"] == "2136.29, -361.34, 97.15"


def test_match_id_jest_deterministyczny(payloads):
    from tests.replay import replay

    first = {e["match_id"] for e in replay(payloads)}
    second = {e["match_id"] for e in replay(payloads)}
    assert len(first) == 1
    assert first == second
