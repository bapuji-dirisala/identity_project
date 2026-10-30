"""DDL for gold layer: identity_hub_dimension (business-facing conformed dim)."""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable


def create_table(table_name: str, spark: SparkSession):
    schema = [
        ("upm_id", "STRING", "Unique profile member id (merge key)"),
        ("email_address_txt", "STRING", "Primary email"),
        ("first_nm", "STRING", "First name"),
        ("last_nm", "STRING", "Last name"),
        ("dob_dt", "DATE", "Date of birth"),
        ("age_nbr", "INT", "Derived age in years"),
        ("legal_marketing_age_ind", "BOOLEAN", "Is user of legal marketing age"),
        ("country_iso2_cd", "STRING", "Country ISO2 code"),
        ("timezone_id", "STRING", "IANA timezone id"),
        ("bot_ind", "BOOLEAN", "Is this user flagged as a bot"),
        ("unsubscribe_ind", "BOOLEAN", "Has user unsubscribed from marketing"),
        ("undeliverable_ind", "BOOLEAN", "Is user's email undeliverable"),
        ("marketing_addressability_ind", "BOOLEAN", "Can we market to this user"),
        ("ltv_scr", "DOUBLE", "Lifetime value score"),
        ("churn_scr", "DOUBLE", "Churn score"),
        ("record_create_tmst_utc", "TIMESTAMP", "First time this row was created"),
        ("record_update_tmst_utc", "TIMESTAMP", "Last update timestamp"),
    ]
    tb = DeltaTable.createIfNotExists(spark).tableName(table_name)
    for n, t, c in schema:
        tb = tb.addColumn(n, dataType=t, nullable=True, comment=c)
    (
        tb.comment("Gold identity hub dimension (streaming + foreachBatch MERGE)")
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.enableDeletionVectors", "true")
        .property("delta.autoOptimize.autoCompact", "true")
        .property("delta.autoOptimize.optimizeWrite", "true")
        .property("delta.deletedFileRetentionDuration", "interval 7 days")
        .property("nike.table.sla.expected_refresh_frequency", "interval 4 hours")
    ).execute()
