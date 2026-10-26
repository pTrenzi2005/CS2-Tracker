"""
Silver: round and match lifecycle events.
`silver.round_events` — round start and end, including the winner.
`silver.match_events` — match open and close.
`win_team` arrives only in the `over` phase, which is not a tick.
"""

from pyspark import pipelines as dp

from cs2_transforms import silver

CATALOG = spark.conf.get("cs2.catalog")
PARSED_TABLE = f"{CATALOG}.silver.gsi_events_parsed"


@dp.table(
    name=f"{CATALOG}.silver.round_events",
    comment="Round start and end, with the winner and player state.",
    cluster_by=["match_id", "round_number"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(silver.ROUND_EVENT_RULES)
@dp.expect_all(silver.ROUND_EVENT_WARNINGS)
def silver_round_events():
    return silver.round_events(spark.readStream.table(PARSED_TABLE))


@dp.table(
    name=f"{CATALOG}.silver.match_events",
    comment="Match open and close detected by the GSI client.",
    cluster_by=["match_id"],
    table_properties={"quality": "silver"},
)
def silver_match_events():
    return silver.match_events(spark.readStream.table(PARSED_TABLE))
