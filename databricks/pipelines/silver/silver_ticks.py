"""
Silver: player ticks from the active round.
`silver.player_ticks` — one row per tick.
"""

from pyspark import pipelines as dp

from cs2_transforms import silver

CATALOG = spark.conf.get("cs2.catalog")
PARSED_TABLE = f"{CATALOG}.silver.gsi_events_parsed"


@dp.table(
    name=f"{CATALOG}.silver.player_ticks",
    comment="Our player's state sampled during the active round.",
    cluster_by=["match_id", "round_number"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(silver.TICK_RULES)
def silver_player_ticks():
    return silver.player_ticks(spark.readStream.table(PARSED_TABLE))
