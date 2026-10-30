CREATE TABLE IF NOT EXISTS ${catalog}.identity_foundation_secure.identity_core (
    upm_id                   STRING,
    email_address_txt        STRING,
    first_nm                 STRING,
    last_nm                  STRING,
    dob_dt                   DATE,
    country_iso2_cd          STRING,
    registration_tmst_utc    TIMESTAMP,
    profile_update_tmst_utc  TIMESTAMP,
    record_load_tmst_utc     TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed'          = 'true',
    'delta.enableDeletionVectors'         = 'true',
    'delta.autoOptimize.autoCompact'      = 'true',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'delta.deletedFileRetentionDuration'  = 'interval 7 days',
    'targetFileSize'                      = '256mb'
);
