"""
Avro / StructType schemas for the bronze (`identity_raw`) layer. In the real
repo this file enumerates every field delivered by the upstream Kafka topic
and is version-pinned to the NSP Schema Registry subject ID.
"""

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

IDENTITY_RAW_SCHEMA = StructType([
    StructField("upm_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_tmst_utc", TimestampType(), True),
    StructField("payload", StringType(), True),  # raw Avro-encoded body
    StructField("source_offset", LongType(), True),
])
