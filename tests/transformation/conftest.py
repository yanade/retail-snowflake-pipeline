import sys
from pathlib import Path
from collections.abc import Iterator
import pytest
from pyspark.sql import SparkSession
from transformation.spark_session import build_local_spark

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "database"))

@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """
    One local Spark session shared by every transformation test.

    scope="session" starts the JVM once per test run instead of once per test.
    """
    session = build_local_spark("retail-pipeline-tests")
    yield session
    session.stop()   # runs after the last test, so the JVM doesn't outlive pytest