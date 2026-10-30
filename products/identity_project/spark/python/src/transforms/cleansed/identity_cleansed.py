'''
Silver cleansing: deserialize + flatten + cast + dedupe raw identity events.

Called by `src/identity_core.py` inside a foreachBatch wrapper.
'''
from pyspark.sql.functions import col, upper, current_timestamp
from delta.tables import DeltaTable


def process_and_merge_micro_batch(microBatchOutputDF, batchId, *, spark, output_table_name):
    # Mock cleansing: ensure email is lowercased, country uppercased, dedupe by upm_id.
    df = (microBatchOutputDF
          .withColumn("email_address_txt", col("email_address_txt"))
          .withColumn("country_iso2_cd",   upper(col("country_iso2_cd")))
          .withColumn("record_load_tmst_utc", current_timestamp())
          .dropDuplicates(["upm_id"]))

    # Upsert into silver.
    delta_tbl = DeltaTable.forName(spark, output_table_name)
    (delta_tbl.alias("tgt")
        .merge(df.alias("src"), "tgt.upm_id = src.upm_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
