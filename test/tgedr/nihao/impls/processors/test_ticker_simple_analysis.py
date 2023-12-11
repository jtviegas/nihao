from test.conftest import assert_frames_are_equal

import pytest
from pyspark.sql import DataFrame, Row

from tgedr.nihao.impls.processors.ticker_simple_analysis import TickerSimpleAnalysis


@pytest.fixture
def input(spark):
    return spark.createDataFrame(
        [
            Row(symbol="AMD", variable="Adj Close", value=3.1458330154418945, actual_time=322099200.0),
            Row(symbol="AMD", variable="Close", value=3.1458330154418945, actual_time=322099200.0),
            Row(symbol="AMD", variable="High", value=3.3020830154418945, actual_time=322099200.0),
            Row(symbol="AMD", variable="Low", value=3.125, actual_time=322099200.0),
            Row(symbol="AMD", variable="Open", value=0.0, actual_time=322099200.0),
            Row(symbol="AMD", variable="Volume", value=219600.0, actual_time=322099200.0),
            Row(symbol="AMD", variable="Adj Close", value=3.03125, actual_time=322185600.0),
            Row(symbol="AMD", variable="Close", value=3.03125, actual_time=322185600.0),
            Row(symbol="AMD", variable="High", value=3.125, actual_time=322185600.0),
            Row(symbol="AMD", variable="Low", value=2.9375, actual_time=322185600.0),
            Row(symbol="AMD", variable="Open", value=0.0, actual_time=322185600.0),
            Row(symbol="AMD", variable="Volume", value=727200.0, actual_time=322185600.0),
        ]
    )


@pytest.fixture
def expected(spark):
    return spark.createDataFrame(
        [
            Row(symbol="AMD", actual_time=322099200.0, adj_close=3.1458330154418945, volume=219600.0),
            Row(symbol="AMD", actual_time=322185600.0, adj_close=3.03125, volume=727200.0),
        ]
    )


def test_process(monkeypatch, spark, input, expected):
    monkeypatch.setattr(spark.read, "parquet", lambda key: input)
    processor = TickerSimpleAnalysis()
    actual: DataFrame = processor.process(input)

    assert_frames_are_equal(actual=actual.toPandas(), expected=expected.toPandas(), sort_columns=["actual_time"])
