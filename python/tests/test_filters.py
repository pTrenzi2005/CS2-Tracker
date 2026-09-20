"""Filtry na nagranych pakietach: nasz gracz i aktywna faza rundy."""

from gsi import filters

OWN_STEAMID = "76561198442208095"


def test_wlasny_gracz_rozpoznany(packet):
    assert filters.is_own_player(packet(50)) is True


def test_kamera_swobodna_bez_steamid_odrzucona(packet):
    # Po smierci blok player to samo spectarget z id encji, bez tozsamosci.
    data = packet(36)
    assert data["player"] == {
        "spectarget": "546",
        "position": "2136.29, -361.34, 97.15",
        "forward": "0.10, 0.99, 0.09",
    }
    assert filters.is_own_player(data) is False


def test_kompletny_blok_obcego_gracza_odrzucony(packet):
    # Grozniejszy przypadek: struktura identyczna jak nasza, inne tylko steamid.
    data = packet(37)
    assert data["player"]["steamid"] != OWN_STEAMID
    assert data["player"]["state"]["round_kills"] == 2
    assert filters.is_own_player(data) is False


def test_freezetime_nie_jest_tickiem(packet):
    data = packet(50)
    assert filters.round_phase(data) == "freezetime"
    assert filters.is_live_tick(data) is False


def test_faza_over_nie_jest_tickiem(packet):
    data = packet(33)
    assert filters.is_own_player(data) is True
    assert filters.round_phase(data) == "over"
    assert filters.is_live_tick(data) is False


def test_menu_nie_jest_meczem(packet):
    data = packet(0)
    assert data["player"]["activity"] == "menu"
    assert filters.is_in_match(data) is False
    assert filters.is_live_tick(data) is False


def test_aktywna_runda_przechodzi(packet):
    data = packet(54)
    assert filters.is_live_tick(data) is True
