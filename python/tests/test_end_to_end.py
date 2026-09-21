"""Test calosciowy: nagrane 194 pakiety przepuszczone przez klienta."""

from collections import Counter

OWN_STEAMID = "76561198442208095"
# Pozycja przeciwnika z pakietu dane_37, w ktorym blok player jest kompletny,
# ale nalezy do kogos innego. Nie moze trafic do zadnego ticka.
FOREIGN_POSITION = "2164.91, 539.17, 160.03"


def test_liczba_zdarzen_wedlug_typu(payloads, envelopes):
    assert len(payloads) == 194
    assert Counter(e["event_type"] for e in envelopes) == {
        "tick": 93,
        "round_start": 7,
        "round_end": 3,
        "match_start": 1,
        "match_end": 1,
    }


def test_wszystkie_zdarzenia_dotycza_nas(envelopes):
    assert {e["steamid"] for e in envelopes} == {OWN_STEAMID}
    positions = [e["payload"]["player"]["position"] for e in envelopes if e["event_type"] == "tick"]
    assert FOREIGN_POSITION not in positions


def test_ticki_tylko_z_aktywnej_rundy(envelopes):
    ticks = [e for e in envelopes if e["event_type"] == "tick"]
    assert {e["payload"]["round"]["phase"] for e in ticks} == {"live"}
    assert {e["payload"]["map"]["phase"] for e in ticks} == {"live"}
    assert {e["payload"]["player"]["activity"] for e in ticks} == {"playing"}


def test_kazde_zdarzenie_ma_komplet_pol_koperty(envelopes):
    wymagane = {
        "schema_version",
        "event_type",
        "event_id",
        "session_id",
        "match_id",
        "seq",
        "client_ts",
        "provider_ts",
        "steamid",
        "round_number",
        "payload",
    }
    for envelope in envelopes:
        assert wymagane <= set(envelope)
        assert envelope["match_id"]


def test_kolejnosc_zdarzen_jest_scisle_rosnaca(envelopes):
    sekwencje = [e["seq"] for e in envelopes]
    assert sekwencje == sorted(sekwencje)
    assert len(set(sekwencje)) == len(sekwencje)


def test_mecz_otwarty_i_zamkniety_dokladnie_raz(envelopes):
    assert envelopes[0]["event_type"] == "match_start"
    assert envelopes[-1]["event_type"] == "match_end"
    assert envelopes[-1]["payload"]["rounds_played"] == 3
