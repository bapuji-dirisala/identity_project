"""DDL for bronze layer: raw identity events from Kafka."""

from pyspark.sql import SparkSession
from ddl.common_ddl_utils import configure_delta_table_builder


def create_table(table_name: str, spark: SparkSession):
    schema = [
        ("key", "BINARY", "Kafka key"),
        ("value", "BINARY", "Kafka value (raw Avro bytes)"),
        ("topic", "STRING", "Kafka topic name"),
        ("partition", "INT", "Kafka partition"),
        ("offset", "LONG", "Kafka offset"),
        ("timestamp", "TIMESTAMP", "Kafka ingestion timestamp"),
        ("record_load_tmst_utc", "TIMESTAMP", "When this row was written to bronze"),
        # Deserialized Avro payload is flattened into columns too (added via mergeSchema).
    ]
    builder = configure_delta_table_builder(
        spark, table_name, schema, "Bronze raw identity events ingested from Kafka"
    )
    builder.execute()
