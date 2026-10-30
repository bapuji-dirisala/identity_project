-- Bootstrap SQL for the bronze table. The streaming notebook also calls
-- `create_table(...)` on start, so this is primarily documentation + a
-- safety net for engineers running one-off backfills via SQL warehouse.
CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA  IF NOT EXISTS ${catalog}.identity_foundation_secure;

CREATE TABLE IF NOT EXISTS ${catalog}.identity_foundation_secure.identity_raw (
    key                   BINARY,
    value                 BINARY,
    topic                 STRING,
    partition             INT,
    offset                LONG,
    timestamp             TIMESTAMP,
    record_load_tmst_utc  TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.enableDeletionVectors'         = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.deletedFileRetentionDuration'  = 'interval 7 days'
);
