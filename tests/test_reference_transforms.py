"""Reference-data parsing on local Spark: map manifest, radar overview files, and the weapon sheet."""

import json

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

import reference_transforms as rt  # noqa: E402

MANIFEST = {
    "count": 2,
    "maps": {
        "de_nuke": {
            "display_name": "Nuke",
            "first_seen": "2023-03-22",
            "path": "https://example.org/icons/de_nuke.png",
            "radar_paths": [
                "https://example.org/radars/de_nuke_lower_radar_psd.png",
                "https://example.org/radars/de_nuke_radar_psd.png",
            ],
            "radar_info": {
                "path": "https://example.org/info/de_nuke.txt",
                "pos_x": -3453.0,
                "pos_y": 2887.0,
                "scale": 7.0,
                "verticalsections": {
                    "default": {"AltitudeMax": 10000.0, "AltitudeMin": -495.0},
                    "lower": {"AltitudeMax": -495.0, "AltitudeMin": -10000.0},
                },
            },
        },
        "ar_shoots": {"display_name": "Shoots", "path": "https://example.org/icons/ar_shoots.png"},
    },
}

OVERVIEW = """
"de_nuke"
{
    "material"  "overviews/de_nuke"
    "pos_x"     "-3453"
    "bombA_x"   "0.5"
    "bombA_y"   "0.25"
    "CTSpawn_x" " 0.8 "
    "CTSpawn_y" "0.4"
}
"""


@pytest.fixture(scope="module")
def silver(spark):
    map_icons = spark.createDataFrame(
        [
            ("map_manifest", None, json.dumps(MANIFEST), "abc123"),
            ("radar_info", "de_nuke", OVERVIEW, "abc123"),
        ],
        "artifact_type STRING, map_name STRING, raw_content STRING, source_commit STRING",
    )
    map_assets = spark.createDataFrame(
        [
            ("icon", "de_nuke", "/Volumes/c/bronze/raw/de_nuke.png", "de_nuke.png"),
            ("radar", "de_nuke", "/Volumes/c/bronze/raw/de_nuke_radar_psd.png", "de_nuke_radar_psd.png"),
        ],
        "asset_type STRING, map_name STRING, volume_path STRING, file_name STRING",
    )
    maps = rt.manifest_maps(map_icons)
    overview = rt.overview(map_icons)
    return {
        "map_reference": rt.map_reference(maps, overview, map_assets, "run1").cache(),
        "map_level": rt.map_level(maps, map_assets, "run1").cache(),
        "map_zone": rt.expect_all_or_drop(
            rt.map_zone(overview, "run1"), "map_zone", rt.MAP_ZONE_RULES
        ).cache(),
    }


def test_map_reference_reads_family_and_radar(silver):
    rows = {r.map_name: r for r in silver["map_reference"].collect()}
    nuke, shoots = rows["de_nuke"], rows["ar_shoots"]
    assert (nuke.map_family, nuke.has_radar, nuke.level_count) == ("bomb_defusal", True, 2)
    assert (nuke.radar_scale, nuke.bombsite_a_x, nuke.ct_spawn_x) == (7.0, 0.5, 0.8)
    assert nuke.radar_material == "overviews/de_nuke"
    assert nuke.icon_volume_path == "/Volumes/c/bronze/raw/de_nuke.png"
    assert (shoots.map_family, shoots.has_radar, shoots.level_count) == ("arms_race", False, 1)
    assert (shoots.radar_rotate, shoots.radar_zoom) == (0.0, 1.0)
    rules = " AND ".join(f"({rule})" for rule in rt.MAP_REFERENCE_RULES.values())
    assert silver["map_reference"].filter(f"NOT coalesce({rules}, false)").count() == 0


def test_map_level_matches_each_level_to_its_radar(silver):
    levels = {r.level_name: r for r in silver["map_level"].collect()}
    assert set(levels) == {"default", "lower"}
    assert levels["default"].radar_url.endswith("/de_nuke_radar_psd.png")
    assert levels["default"].radar_volume_path == "/Volumes/c/bronze/raw/de_nuke_radar_psd.png"
    assert levels["lower"].radar_url.endswith("/de_nuke_lower_radar_psd.png")
    assert levels["default"].is_default_level is True
    assert (levels["lower"].altitude_min, levels["lower"].altitude_max) == (-10000.0, -495.0)


def test_zones_without_a_position_are_dropped(silver):
    zones = {r.zone_type: (r.frac_x, r.frac_y) for r in silver["map_zone"].collect()}
    assert zones == {"bombsite_a": (0.5, 0.25), "ct_spawn": (0.8, 0.4)}


def test_zone_world_coordinates_flip_y(silver):
    zones = {
        r.zone_type: (r.world_x, r.world_y)
        for r in rt.dim_map_zone(silver["map_zone"], silver["map_reference"]).collect()
    }
    # pos + frac * scale * 1024; y counts down from the top edge.
    assert zones["bombsite_a"] == (131.0, 1095.0)


def test_radar_world_bounds(silver):
    nuke = rt.dim_map_reference(silver["map_reference"]).filter("map_name = 'de_nuke'").collect()[0]
    assert (nuke.world_min_x, nuke.world_max_x) == (-3453.0, -3453.0 + 7 * 1024)
    assert (nuke.world_min_y, nuke.world_max_y) == (2887.0 - 7 * 1024, 2887.0)


def weapon_line(name: str, price: str, damage: int, cycle_time: float) -> str:
    leading = [name, "0.47", str(damage), "0.85", "4", str(cycle_time), "1", "$300", "240",
               "20", price, "4096", "Pistol", "0", "1"]
    columns = len(rt.WEAPON_CSV_SCHEMA.split(", "))
    return ",".join(leading + ["1"] * (columns - len(leading)))


def test_weapon_sheet_parsing_and_spec(spark):
    lines = [
        "Pistols,WeaporArmorRatio,Damage",
        weapon_line("glock", "$200", 30, 0.15),
        ",,,,",
        weapon_line("broken", "$0", 0, 0.1),
    ]
    raw = spark.createDataFrame(
        [("weapons_raw_values.csv", i + 1, line, "run1") for i, line in enumerate(lines)]
        + [("weapons_simple.csv", 1, "ignored", "run1")],
        "file_name STRING, line_number INT, raw_line STRING, ingest_run_id STRING",
    )
    weapons = rt.expect_all_or_drop(rt.weapon_attributes(raw), "weapon_attributes", rt.WEAPON_RULES)
    rows = weapons.collect()
    assert [r.weapon_name for r in rows] == ["glock"]
    glock = rows[0]
    assert (glock.gsi_weapon_name, glock.weapon_category) == ("weapon_glock", "Pistols")
    assert (glock.price_usd, glock.kill_award_usd) == (200, 300)

    spec = rt.dim_weapon_spec(weapons).collect()[0]
    assert (spec.fire_rate_rpm, spec.damage_per_second) == (400.0, 200.0)
    assert spec.kills_with_one_headshot is True
