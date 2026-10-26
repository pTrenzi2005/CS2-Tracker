"""
Silver: parsed GSI event envelopes.
`silver.gsi_events_parsed` — one row per event, deduplicated.
"""

from pyspark import pipelines as dp

from cs2_transforms import silver

CATALOG = spark.conf.get("cs2.catalog")
BRONZE_TABLE = f"{CATALOG}.bronze.gsi_raw_events"


@dp.table(
    name=f"{CATALOG}.silver.gsi_events_parsed",
    comment="GSI event envelope parsed from raw JSON and deduplicated.",
    cluster_by=["match_id", "event_type"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(silver.EVENT_RULES)
def silver_gsi_events_parsed():
    return (
        silver.parse_envelopes(spark.readStream.table(BRONZE_TABLE))
        .withWatermark("client_ts", "2 hours")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
