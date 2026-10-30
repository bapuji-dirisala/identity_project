# Databricks notebook source
# MAGIC %md
# MAGIC # identity_core
# MAGIC Silver layer: streaming CDC from `identity_raw`, cleanses + flattens,
# MAGIC MERGEs into `identity_core` on upm_id.
# MAGIC - Trigger: 120s, maxFilesPerTrigger=10
# MAGIC - Checkpoint on UC Volume for exactly-once sink

from transforms.cleansed.identity_cleansed import process_and_merge_micro_batch
from ddl.identity_core_ddl import create_table

dbutils.widgets.text("environment",         "development")
dbutils.widgets.text("source_catalog",      "development")
dbutils.widgets.text("destination_catalog", "development")
dbutils.widgets.text("source_schema",       "identity_foundation_secure")
dbutils.widgets.text("target_schema",       "identity_foundation_secure")
dbutils.widgets.text("source_table_name",   "identity_raw")
dbutils.widgets.text("target_table_name",   "identity_core")
dbutils.widgets.text("checkpoint_version",  "0")
dbutils.widgets.text("maxFilesPerTrigger",  "10")
dbutils.widgets.text("process_time_interval","120 seconds")

env           = dbutils.widgets.get("environment")
src_catalog   = dbutils.widgets.get("source_catalog")
dest_catalog  = dbutils.widgets.get("destination_catalog")
src_schema    = dbutils.widgets.get("source_schema")
tgt_schema    = dbutils.widgets.get("target_schema")
src_table     = dbutils.widgets.get("source_table_name")
tgt_table     = dbutils.widgets.get("target_table_name")
ck_version    = dbutils.widgets.get("checkpoint_version")
max_files     = int(dbutils.widgets.get("maxFilesPerTrigger"))
proc_interval = dbutils.widgets.get("process_time_interval")

src_full = f"{src_catalog}.{src_schema}.{src_table}"
tgt_full = f"{dest_catalog}.{tgt_schema}.{tgt_table}"
checkpoint = f"/Volumes/{dest_catalog}/{tgt_schema}/{tgt_table}_v{ck_version}/"

create_table(tgt_full, spark)

input_df = (spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", max_files)
    .option("skipChangeCommits", "true")        # Silver reads CDC changes but ignores deletes/updates markers
    .option("schemaEvolutionMode", "addNewColumns")
    .table(src_full))


def _batch(df, batchId):
    process_and_merge_micro_batch(df, batchId, spark=spark, output_table_name=tgt_full)


(input_df.writeStream
    .foreachBatch(_batch)
    .option("checkpointLocation", checkpoint)
    .outputMode("update")
    .trigger(processingTime=proc_interval)
    .queryName("identity_core")
    .start())
