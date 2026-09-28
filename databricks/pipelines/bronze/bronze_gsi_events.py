"""
Bronze: raw GSI events from Azure Event Hubs.
`bronze.gsi_raw_events` — one row per Kafka message. The payload stays as text.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("cs2.catalog")
EVH_NAMESPACE = spark.conf.get("cs2.eventhub.namespace")
EVH_NAME = spark.conf.get("cs2.eventhub.name")
SECRET_SCOPE = spark.conf.get("cs2.secret_scope")
SECRET_KEY = spark.conf.get("cs2.eventhub.secret_key", "evh-connection-string")
STARTING_OFFSETS = spark.conf.get("cs2.eventhub.starting_offsets", "earliest")

CONNECTION_STRING = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

JAAS_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{CONNECTION_STRING}";'
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EVH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EVH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": JAAS_CONFIG,
    "startingOffsets": STARTING_OFFSETS,
    "failOnDataLoss": "false",
}


@dp.table(
    name=f"{CATALOG}.bronze.gsi_raw_events",
    comment="Raw GSI events from Event Hubs. Payload is unchanged.",
    cluster_by=["ingest_date"],
    table_properties={"quality": "bronze"},
)
def bronze_gsi_events():
    return (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
        .select(
            F.col("value").cast("string").alias("raw_payload"),
            F.col("key").cast("string").alias("partition_key"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )
