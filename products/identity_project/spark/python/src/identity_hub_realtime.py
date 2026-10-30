# Databricks notebook source
# MAGIC %md
# MAGIC # identity_hub_realtime
# MAGIC Gold layer: streaming merge from `identity_core` + 10 batch enrichment deltas
# MAGIC into `identity_hub_dimension`. foreachBatch orchestrates the enrichments and
# MAGIC maintains a version-stats table so each source only re-merges when its Delta
# MAGIC version advances AND the per-source refresh interval has elapsed.
# MAGIC - Trigger: 60s, maxFilesPerTrigger=50
# MAGIC - Checkpoint on UC Volume (exactly-once sink)

import yaml
from datetime import datetime
from delta.tables import DeltaTable

from ddl.identity_hub_ddl import create_table
from transforms.hub.identity_hub import process_and_merge_micro_batch

dbutils.widgets.text("environment",         "development")
dbutils.widgets.text("source_catalog",      "development")
dbutils.widgets.text("destination_catalog", "development")
dbutils.widgets.text("source_schema",       "identity_foundation_secure")
dbutils.widgets.text("target_schema",       "identity_foundation_secure")
dbutils.widgets.text("source_table_name",   "identity_core")
dbutils.widgets.text("target_table_name",   "identity_hub_dimension")
dbutils.widgets.text("checkpoint_version",  "0")
dbutils.widgets.text("maxFilesPerTrigger",  "50")
dbutils.widgets.text("process_time_interval","60 seconds")
dbutils.widgets.text("source_stats_table",  "identity_hub_realtime_sources_stats")

env            = dbutils.widgets.get("environment")
src_catalog    = dbutils.widgets.get("source_catalog")
dest_catalog   = dbutils.widgets.get("destination_catalog")
src_schema     = dbutils.widgets.get("source_schema")
tgt_schema     = dbutils.widgets.get("target_schema")
src_table      = dbutils.widgets.get("source_table_name")
tgt_table      = dbutils.widgets.get("target_table_name")
ck_version     = dbutils.widgets.get("checkpoint_version")
max_files      = int(dbutils.widgets.get("maxFilesPerTrigger"))
proc_interval  = dbutils.widgets.get("process_time_interval")
stats_table    = dbutils.widgets.get("source_stats_table")

src_full   = f"{src_catalog}.{src_schema}.{src_table}"
tgt_full   = f"{dest_catalog}.{tgt_schema}.{tgt_table}"
stats_full = f"{dest_catalog}.{tgt_schema}.{stats_table}"
checkpoint = f"/Volumes/{dest_catalog}/{tgt_schema}/{tgt_table}_v{ck_version}/"

# Ensure target table exists
create_table(tgt_full, spark)

# Load enrichment config
with open("resources/config/identity_hub.yaml") as f:
    source_tables_config = yaml.safe_load(f)

# Resume per-source state from the stats table
try:
    stats_df = spark.table(stats_full)
    batch_source_table_version_stats_map = {
        row["source_table_name"]: (row["last_read_version"], row["last_merge_timestamp"])
        for row in stats_df.collect()
    }
except Exception:
    batch_source_table_version_stats_map = {}
next_check_time = {}

# Main source stream: silver -> gold
input_df = (spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", max_files)
    .option("skipChangeCommits", "true")
    .option("schemaEvolutionMode", "addNewColumns")
    .table(src_full))


def _batch(df, batchId):
    process_and_merge_micro_batch(
        spark=spark,
        microBatchOutputDF=df,
        batchId=batchId,
        output_table_name=tgt_full,
        batch_source_table_version_stats_map=batch_source_table_version_stats_map,
        next_check_time=next_check_time,
        source_tables_config=source_tables_config,
        destination_catalog=dest_catalog,
    )


(input_df.writeStream
    .foreachBatch(_batch)
    .option("checkpointLocation", checkpoint)
    .outputMode("update")
    .trigger(processingTime=proc_interval)
    .queryName("identity_hub_realtime")
    .start())
