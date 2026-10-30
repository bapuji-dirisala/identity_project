"""Shared pytest fixtures - spin up a local SparkSession with Delta support."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Local Spark for scaffold tests. Delta extensions are intentionally not
    # configured here so CI doesn't need to download the delta-spark jars.
    s = SparkSession.builder.master("local[2]").appName("identity-tests").getOrCreate()
    yield s
    s.stop()
