# Databricks notebook source
# MAGIC %md
# MAGIC # identity_raw
# MAGIC Bronze layer: raw Kafka events -> Delta.
# MAGIC - Trigger: continuous, maxFilesPerTrigger=5
# MAGIC - Cluster: m7gd.2xlarge, 1-5 autoscale (defined in Brickflow)
# MAGIC - Schema evolution: mergeSchema=true

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col
from ddl.identity_raw_ddl import create_table
from utils.schema.schema_registry import SchemaRegistry

# RETRIEVE WIDGETS -----------------------------------------------------------
dbutils.widgets.text("environment", "development")
dbutils.widgets.text("destination_catalog", "development")
dbutils.widgets.text("target_schema", "identity_foundation_secure")
dbutils.widgets.text("target_table_name", "identity_raw")
dbutils.widgets.text("checkpoint_version", "0")
dbutils.widgets.text("client_secret_scope", "<SECRET_SCOPE>")
dbutils.widgets.text("client_secret_key", "kafka.client_secret")
dbutils.widgets.text("client_id", "<KAFKA_CLIENT_ID>")
dbutils.widgets.text("oauthendpoint", "<OKTA_OAUTH_URL>")
dbutils.widgets.text("kafkatopic", "identity-events")
dbutils.widgets.text("kafkaservers", "<KAFKA_BROKERS>")
dbutils.widgets.text("schema_registry_url", "<SCHEMA_REGISTRY_URL>")
dbutils.widgets.text("schema_version_id_v1", "1000")
dbutils.widgets.text("schema_version_id_v2", "1001")
dbutils.widgets.text("available_now", "false")

env = dbutils.widgets.get("environment")
dest_catalog = dbutils.widgets.get("destination_catalog")
schema = dbutils.widgets.get("target_schema")
table_name = dbutils.widgets.get("target_table_name")
ck_version = dbutils.widgets.get("checkpoint_version")
secret_scope = dbutils.widgets.get("client_secret_scope")
secret_key = dbutils.widgets.get("client_secret_key")
client_secret = dbutils.secrets.get(scope=secret_scope, key=secret_key)
client_id = dbutils.widgets.get("client_id")
okta_url = dbutils.widgets.get("oauthendpoint")
topic = dbutils.widgets.get("kafkatopic")
brokers = dbutils.widgets.get("kafkaservers")
sr_url = dbutils.widgets.get("schema_registry_url")
sv1 = int(dbutils.widgets.get("schema_version_id_v1"))
sv2 = int(dbutils.widgets.get("schema_version_id_v2"))
available_now = dbutils.widgets.get("available_now") == "true"

# Resolve Avro schemas from the schema registry (versions evolve independently of code deploys)
sr = SchemaRegistry(
    okta_url, client_id, client_secret, "client_credentials", sr_url, topic + "-value"
)
schema_v1 = sr.get_schema_for_given_version(sv1)
schema_v2 = sr.get_schema_for_given_version(sv2)

target_full = f"{dest_catalog}.{schema}.{table_name}"
checkpoint = f"/Volumes/{dest_catalog}/{schema}/{table_name}_v{ck_version}/"

# Streaming read: Kafka with Okta client-credentials OAuth
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "OAUTHBEARER")
    .load()
)

# Ensure the sink table exists with the standard property set
create_table(target_full, spark)

# Write to Delta (append-only; schema evolves additively)
query = (
    raw_stream.withColumn("record_load_tmst_utc", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    if available_now
    else raw_stream.withColumn("record_load_tmst_utc", current_timestamp())
    .writeStream.format("delta")
    .option("checkpointLocation", checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
)

query.toTable(target_full)
