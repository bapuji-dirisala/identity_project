"""Smoke test for the gold-layer merge helpers."""

from pyspark.sql import Row


def test_target_columns_cache(spark):
    # Create a tiny Delta table and verify get_target_columns returns its schema
    df = spark.createDataFrame([Row(upm_id="u1", email="a@b.com")])
    # Scaffold CI runs without Delta jars; parquet is enough to exercise get_target_columns
    df.write.format("parquet").mode("overwrite").saveAsTable("test_identity_hub_tmp")

    from transforms.hub.identity_hub import get_target_columns, _target_columns_cache

    _target_columns_cache.clear()
    cols = get_target_columns(spark, "test_identity_hub_tmp")
    assert "upm_id" in cols
    assert "email" in cols
