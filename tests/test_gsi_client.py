from gsi import filters
from gsi.envelope import (
    EVENT_MATCH_END,
    EVENT_MATCH_START,
    EVENT_ROUND_END,
    EVENT_ROUND_START,
    EVENT_TICK,
    EnvelopeFactory,
    GsiEvent,
    active_weapon_name,
    build_player_snapshot,
    normalize_weapons,
)
from gsi.match_state import MatchTracker
from payloads import OTHER_STEAMID, packet, short_match


def run(packets):
    tracker = MatchTracker()
    events = []
    for payload in packets:
        events.extend(tracker.process(payload))
    return tracker, events


def test_live_tick_needs_own_player_and_live_phases():
    assert filters.is_live_tick(packet(ts=1))
    assert not filters.is_live_tick(packet(ts=1, player_steamid=OTHER_STEAMID))
    assert not filters.is_live_tick(packet(ts=1, round_phase="freezetime"))
    assert not filters.is_live_tick(packet(ts=1, map_phase="warmup"))


def test_weapons_become_an_array_ordered_by_slot():
    weapons = normalize_weapons(packet(ts=1)["player"]["weapons"])
    assert [w["slot"] for w in weapons] == [0, 1]
    assert active_weapon_name(weapons) == "weapon_ak47"
    assert normalize_weapons(None) == []


def test_player_snapshot_unescapes_the_name():
    assert build_player_snapshot(packet(ts=1))["name"] == "Tester & Co"


def test_first_live_packet_opens_match_and_round():
    _, events = run([packet(ts=1000)])
    assert [e.event_type for e in events] == [EVENT_MATCH_START, EVENT_ROUND_START, EVENT_TICK]
    assert all(e.match_id == events[0].match_id for e in events)
    assert events[1].round_number == 0


def test_short_match_lifecycle():
    _, events = run(short_match())
    types = [e.event_type for e in events]
    assert types == [
        EVENT_MATCH_START,
        EVENT_ROUND_START,
        EVENT_TICK,
        EVENT_TICK,
        EVENT_ROUND_END,
        EVENT_MATCH_END,
    ]
    round_end = events[4]
    assert round_end.round_number == 0
    assert round_end.body["win_team"] == "CT"
    assert round_end.body["win_reason"] == "ct_win_elimination"
    assert round_end.body["player_survived"] is True
    assert events[-1].body["rounds_played"] == 1


def test_match_id_is_stable_for_a_replay():
    _, first = run(short_match())
    _, second = run(short_match())
    assert first[0].match_id == second[0].match_id


def test_map_change_opens_a_new_match():
    _, events = run([packet(ts=1000), packet(ts=1001, map_name="de_mirage")])
    starts = [e for e in events if e.event_type == EVENT_MATCH_START]
    assert len(starts) == 2
    assert starts[0].match_id != starts[1].match_id
    assert EVENT_MATCH_END in [e.event_type for e in events]


def test_spectated_player_produces_no_ticks():
    _, events = run([packet(ts=1000, player_steamid=OTHER_STEAMID)])
    assert EVENT_TICK not in [e.event_type for e in events]


def test_finalize_closes_an_open_match():
    tracker, _ = run([packet(ts=1000)])
    assert [e.event_type for e in tracker.finalize()] == [EVENT_MATCH_END]
    assert tracker.finalize() == []


def test_envelope_assigns_sequence_and_ids():
    factory = EnvelopeFactory(session_id="s1")
    event = GsiEvent("tick", "m1", "sid", 0, 1000, {"a": 1})
    first = factory.wrap(event, client_ts="2026-01-01T00:00:00.000Z")
    second = factory.wrap(event)
    assert (first["seq"], second["seq"]) == (1, 2)
    assert first["event_id"] != second["event_id"]
    assert first["session_id"] == "s1"
    assert first["client_ts"] == "2026-01-01T00:00:00.000Z"
    assert second["client_ts"].endswith("Z")
    assert first["payload"] == {"a": 1}
