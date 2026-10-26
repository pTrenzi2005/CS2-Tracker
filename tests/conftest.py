import os
import sys

import pytest


# Test modules import the transforms at collection time, before any fixture runs. Module-level code there must
# not build Columns or Windows: classic PySpark creates them in the JVM, which needs an active SparkContext.
@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # Python UDF workers would otherwise start whatever python is first on PATH, which may lack PySpark.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    session = (
        SparkSession.builder.master("local[1]")
        .appName("cs2-tracker-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
