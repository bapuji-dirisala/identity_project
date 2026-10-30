"""
Gold-layer merge functions (the heart of identity_hub_realtime).

Each `merge_results_*` function pulls deltas from one upstream enrichment
table and merges them into `identity_hub_dimension` on `upm_id`. The
foreachBatch orchestrator in `src/identity_hub_realtime.py` iterates all of
them per micro-batch, honouring per-source refresh intervals from
`resources/config/identity_hub.yaml` to avoid wasting work.
"""

from pyspark.sql.functions import (
    col,
    current_timestamp,
    max as spark_max,
    struct,
    coalesce,
    lit,
)
from delta.tables import DeltaTable
from datetime import datetime


# -----------------------------------------------------------------------------
# Core helpers
# -----------------------------------------------------------------------------

_target_columns_cache = {}


def get_target_columns(spark, target_table_name):
    # Uses spark.table(...).schema so this works for any registered table (Delta,
    # Parquet, etc.) and doesn't require the Delta runtime jars in local tests.
    if target_table_name not in _target_columns_cache:
        _target_columns_cache[target_table_name] = [
            f.name for f in spark.table(target_table_name).schema.fields
        ]
    return _target_columns_cache[target_table_name]


def get_max_version_of_source_table(spark, source_delta_table_name):
    return (
        DeltaTable.forName(spark, source_delta_table_name)
        .history(1)
        .selectExpr("max(version) AS v")
        .collect()[0][0]
    )


def generic_common_merge_function(
    spark, df, update_set, output_table_name, key_column="upm_id"
):
    """Perform an idempotent MERGE on the gold table."""
    tgt = DeltaTable.forName(spark, output_table_name)
    (
        tgt.alias("output_delta_table")
        .merge(df.alias("df"), f"output_delta_table.{key_column} = df.{key_column}")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values={**update_set, key_column: f"df.{key_column}"})
        .execute()
    )


# -----------------------------------------------------------------------------
# Per-source merge functions (illustrative - add more as new enrichment sources arrive)
# -----------------------------------------------------------------------------


def merge_results_core(spark, input_df, output_table_name):
    """Merge the main silver stream into gold - the base rows."""
    target_columns = get_target_columns(spark, output_table_name)
    update_set = {
        c: f"df.{c}" for c in input_df.columns if c != "upm_id" and c in target_columns
    }
    update_set["record_update_tmst_utc"] = "current_timestamp()"
    generic_common_merge_function(spark, input_df, update_set, output_table_name)


def merge_results_bots(**kwargs):
    """Enrichment: `bot_ind` boolean from the bots delta table."""
    spark = kwargs["spark"]
    delta_table_name = kwargs["delta_table_name"]
    output_table_name = kwargs["output_table_name"]

    src = spark.table(delta_table_name).select("upm_id", "bot_ind")
    target_columns = get_target_columns(spark, output_table_name)
    update_set = {
        c: f"df.{c}" for c in src.columns if c != "upm_id" and c in target_columns
    }
    generic_common_merge_function(spark, src, update_set, output_table_name)


def merge_results_timezone(**kwargs):
    """Enrichment: timezone_id from a daily timezone resolver."""
    spark = kwargs["spark"]
    delta_table_name = kwargs["delta_table_name"]
    output_table_name = kwargs["output_table_name"]

    src = spark.table(delta_table_name).select("upm_id", "timezone_id")
    target_columns = get_target_columns(spark, output_table_name)
    update_set = {
        c: f"df.{c}" for c in src.columns if c != "upm_id" and c in target_columns
    }
    generic_common_merge_function(spark, src, update_set, output_table_name)


# -----------------------------------------------------------------------------
# foreachBatch orchestrator (called from identity_hub_realtime notebook)
# -----------------------------------------------------------------------------


def should_check_source(name, next_check_time, refresh_interval, last_merge_ts):
    now = datetime.now()
    if name not in next_check_time:
        next_check_time[name] = last_merge_ts
    nxt = next_check_time[name]
    if nxt is not None and now < nxt:
        return False
    from datetime import timedelta

    next_check_time[name] = now + timedelta(seconds=refresh_interval)
    return True


def merge_identity_hub_gold(
    spark,
    input_df,
    output_table_name,
    batch_source_table_version_stats_map,
    next_check_time,
    source_tables_config,
    destination_catalog,
):
    """Called from foreachBatch - merges core + all enrichment sources."""

    # 1) Core silver -> gold merge (every batch).
    merge_results_core(spark, input_df, output_table_name)

    # 2) Walk each enrichment source config; only merge if its Delta version advanced
    #    AND the per-source refresh interval has elapsed.
    for src_cfg in source_tables_config or []:
        name = list(src_cfg.keys())[0]
        cfg = src_cfg[name]
        src_table = cfg["source_table_name"]
        func_name = cfg["source_table_merge_function_name"]
        refresh_interval = cfg["refresh_interval"]
        last_read_version, last_merge_ts = batch_source_table_version_stats_map.get(
            name, (None, None)
        )

        if not should_check_source(
            name, next_check_time, refresh_interval, last_merge_ts
        ):
            continue
        try:
            max_v = get_max_version_of_source_table(spark, src_table)
        except Exception:
            continue
        if max_v == last_read_version:
            continue
        func = globals().get(func_name)
        if not func:
            print(f"Function '{func_name}' not found; skipping {name}")
            continue
        func(
            spark=spark, delta_table_name=src_table, output_table_name=output_table_name
        )
        batch_source_table_version_stats_map[name] = (max_v, datetime.now())
        spark.sql(
            f"UPDATE {destination_catalog}.<SCHEMA>.identity_hub_realtime_sources_stats "
            f"SET last_read_version = {max_v}, "
            f"last_merge_timestamp = '{datetime.now()}' "
            f"WHERE source_table_name = '{name}'"
        )


def process_and_merge_micro_batch(
    spark,
    microBatchOutputDF,
    batchId,
    output_table_name,
    batch_source_table_version_stats_map,
    next_check_time,
    source_tables_config,
    destination_catalog,
):
    merge_identity_hub_gold(
        spark,
        microBatchOutputDF,
        output_table_name,
        batch_source_table_version_stats_map,
        next_check_time,
        source_tables_config,
        destination_catalog,
    )
