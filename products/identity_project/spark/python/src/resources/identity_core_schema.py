"""
StructType schema for the silver (`identity_core`) layer: fully-decoded,
typed, deduplicated identity rows.
"""

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

IDENTITY_CORE_SCHEMA = StructType([
    StructField("upm_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("record_create_tmst_utc", TimestampType(), True),
    StructField("record_update_tmst_utc", TimestampType(), True),
])
