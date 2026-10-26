"""The GSI client's envelopes through silver and gold, on local Spark."""

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from cs2_transforms import gold, silver  # noqa: E402
from gsi.envelope import EnvelopeFactory  # noqa: E402
from gsi.match_state import MatchTracker  # noqa: E402
from payloads import STEAMID, short_match  # noqa: E402


def failing(df, rules):
    condition = " AND ".join(f"({rule})" for rule in rules.values())
    return df.filter(f"NOT coalesce({condition}, false)").count()


@pytest.fixture(scope="module")
def layers(spark):
    tracker = MatchTracker()
    factory = EnvelopeFactory(session_id="test-session")
    events = [event for payload in short_match() for event in tracker.process(payload)]
    rows = [
        (
            json.dumps(factory.wrap(event, client_ts=f"2026-01-01T00:00:{index:02d}.000Z")),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            index,
        )
        for index, event in enumerate(events)
    ]
    raw = spark.createDataFrame(rows, "raw_payload STRING, ingest_ts TIMESTAMP, kafka_offset BIGINT")

    parsed = silver.parse_envelopes(raw).cache()
    ticks = silver.player_ticks(parsed).cache()
    rounds = silver.round_events(parsed).cache()
    matches = silver.match_events(parsed)
    kills = silver.kill_events(ticks)
    deaths = silver.death_events(ticks, rounds)
    damage = silver.damage_events(ticks)
    state = silver.round_player_state(ticks)
    dim_round = gold.dim_round(rounds, state).cache()
    dim_match = gold.dim_match(dim_round, matches)
    stats = gold.fact_round_stats(dim_round, state, kills, deaths, damage, rounds).cache()
    return {
        "parsed": parsed,
        "ticks": ticks,
        "rounds": rounds,
        "matches": matches,
        "kills": kills,
        "deaths": deaths,
        "damage": damage,
        "state": state,
        "dim_round": dim_round,
        "dim_match": dim_match,
        "stats": stats,
        "summary": gold.agg_match_summary(stats, dim_match),
    }


def test_every_client_envelope_passes_silver_rules(layers):
    parsed = layers["parsed"]
    assert parsed.count() == 6
    assert failing(parsed, silver.EVENT_RULES) == 0
    assert parsed.filter("steamid IS NULL OR client_ts IS NULL").count() == 0


def test_ticks_are_typed_and_pass_rules(layers):
    ticks = layers["ticks"]
    assert ticks.count() == 2
    assert failing(ticks, silver.TICK_RULES) == 0
    last = ticks.orderBy("seq").collect()[-1]
    assert (last.pos_x, last.pos_y, last.pos_z) == (3.0, 4.0, 0.0)
    assert last.yaw_deg == 0.0
    assert last.active_weapon == "weapon_ak47"
    assert [w.name for w in last.weapons] == ["weapon_knife", "weapon_ak47"]


def test_round_and_match_events(layers):
    assert sorted(r.event_type for r in layers["rounds"].collect()) == ["round_end", "round_start"]
    assert failing(layers["rounds"], silver.ROUND_EVENT_RULES) == 0
    assert failing(layers["rounds"], silver.ROUND_EVENT_WARNINGS) == 0
    assert sorted(r.event_type for r in layers["matches"].collect()) == ["match_end", "match_start"]


def test_kill_and_damage_come_from_counter_deltas(layers):
    kills = layers["kills"].collect()
    assert len(kills) == 1
    assert (kills[0].kill_count, kills[0].headshot_count, kills[0].weapon_name) == (
        1,
        1,
        "weapon_ak47",
    )
    damage = {r.damage_type: r.damage for r in layers["damage"].collect()}
    assert damage == {"dealt": 100, "taken": 20}
    assert layers["deaths"].count() == 0


def test_round_state_tracks_distance_and_economy(layers):
    state = layers["state"].collect()[0]
    assert state.tick_count == 2
    assert state.distance_units == 5.0
    assert state.equip_value_max == 800


def test_dim_round_classifies_the_round(layers):
    row = layers["dim_round"].collect()[0]
    assert row.player_team == "CT"
    assert row.player_won is True
    assert row.player_survived is True
    assert row.is_pistol_round is True
    assert row.round_type == "eco"


def test_match_result_and_kpis(layers):
    match = layers["dim_match"].collect()[0]
    assert (match.rounds_played, match.rounds_won, match.match_result) == (1, 1, "win")
    stats = layers["stats"].collect()[0]
    assert (stats.kills, stats.headshot_kills, stats.deaths) == (1, 1, 0)
    assert (stats.damage_dealt, stats.damage_taken) == (100, 20)
    summary = layers["summary"].collect()[0]
    assert (summary.kd_ratio, summary.adr, summary.headshot_pct) == (1.0, 100.0, 100.0)


def test_facts_measure_time_from_round_start(layers):
    facts = gold.fact_kills(layers["kills"], layers["dim_round"]).collect()
    assert facts[0].time_in_round_s == 2.0


def test_player_key_hides_the_steamid(layers):
    players = gold.dim_player(layers["ticks"]).collect()
    assert len(players) == 1
    assert STEAMID not in players[0].steamid_masked
    assert players[0].player_name == "Tester & Co"


def test_dim_weapon_classes(layers):
    classes = {r.weapon_name: r.weapon_class for r in gold.dim_weapon(layers["ticks"]).collect()}
    assert classes == {"weapon_ak47": "primary", "weapon_knife": "melee"}


def test_round_type_thresholds(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(None,), (1000,), (2500,), (5000,)], "equip INT")
    types = [r.t for r in df.select(gold.round_type(F.col("equip")).alias("t")).collect()]
    assert types == ["unknown", "eco", "force", "full"]
