"""Synthetic GSI packets for one short match: a round with a headshot kill, then game over."""

from __future__ import annotations

from typing import Any

STEAMID = "76561198000000001"
OTHER_STEAMID = "76561198000000002"
MAP_NAME = "de_dust2"


def packet(
    *,
    ts: int,
    map_phase: str = "live",
    map_round: int = 0,
    round_phase: str = "live",
    win_team: str | None = None,
    round_wins: dict[str, str] | None = None,
    map_name: str = MAP_NAME,
    player_steamid: str = STEAMID,
    team: str = "CT",
    health: int = 100,
    round_kills: int = 0,
    round_killhs: int = 0,
    round_totaldmg: int = 0,
    match_kills: int = 0,
    money: int = 800,
    equip_value: int = 800,
    position: str = "0.00, 0.00, 0.00",
) -> dict[str, Any]:
    round_block: dict[str, Any] = {"phase": round_phase}
    if win_team:
        round_block["win_team"] = win_team
    map_block: dict[str, Any] = {
        "name": map_name,
        "mode": "competitive",
        "phase": map_phase,
        "round": map_round,
        "team_ct": {"score": 0, "consecutive_round_losses": 0},
        "team_t": {"score": 0, "consecutive_round_losses": 0},
    }
    if round_wins:
        map_block["round_wins"] = round_wins
    return {
        "provider": {"steamid": STEAMID, "timestamp": ts},
        "map": map_block,
        "round": round_block,
        "player": {
            "steamid": player_steamid,
            "name": "Tester &amp; Co",
            "team": team,
            "activity": "playing",
            "observer_slot": 1,
            "state": {
                "health": health,
                "armor": 100,
                "helmet": True,
                "flashed": 0,
                "smoked": 0,
                "burning": 0,
                "money": money,
                "round_kills": round_kills,
                "round_killhs": round_killhs,
                "round_totaldmg": round_totaldmg,
                "equip_value": equip_value,
            },
            "match_stats": {"kills": match_kills, "assists": 0, "deaths": 0, "mvps": 0, "score": 0},
            "weapons": {
                "weapon_1": {"name": "weapon_ak47", "type": "Rifle", "state": "active"},
                "weapon_0": {"name": "weapon_knife", "type": "Knife", "state": "holstered"},
            },
            "position": position,
            "forward": "1.00, 0.00, 0.00",
        },
    }


def short_match() -> list[dict[str, Any]]:
    """Round 0: spawn, a headshot kill while taking 20 damage, CT wins. Then game over."""
    kill = dict(
        health=80,
        round_kills=1,
        round_killhs=1,
        round_totaldmg=100,
        match_kills=1,
        position="3.00, 4.00, 0.00",
    )
    return [
        packet(ts=1000),
        packet(ts=1001, **kill),
        packet(
            ts=1002,
            map_round=1,
            round_phase="over",
            win_team="CT",
            round_wins={"1": "ct_win_elimination"},
            **kill,
        ),
        packet(ts=1003, map_phase="gameover", map_round=1, round_phase="over", **kill),
    ]
