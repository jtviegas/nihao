import pytest
import yfinance as yf
from pandas import DataFrame, Timestamp

from tgedr.nihao.commons.utils import assert_frames_are_equal
from tgedr.nihao.source.yahoo_tickers import YahooTickersSource


@pytest.fixture
def data() -> DataFrame:
    return DataFrame(
        {
            ("Adj Close", "AMZN"): {
                Timestamp("1997-05-15 00:00:00"): 0.09791699796915054,
                Timestamp("1997-05-16 00:00:00"): 0.0864579975605011,
                Timestamp("1997-05-19 00:00:00"): 0.0854170024394989,
                Timestamp("1997-05-20 00:00:00"): 0.08177100121974945,
                Timestamp("1997-05-21 00:00:00"): 0.07135400176048279,
            },
            ("Adj Close", "GOOG"): {
                Timestamp("1997-05-15 00:00:00"): float("nan"),
                Timestamp("1997-05-16 00:00:00"): float("nan"),
                Timestamp("1997-05-19 00:00:00"): float("nan"),
                Timestamp("1997-05-20 00:00:00"): float("nan"),
                Timestamp("1997-05-21 00:00:00"): float("nan"),
            },
            ("Close", "AMZN"): {
                Timestamp("1997-05-15 00:00:00"): 0.09791699796915054,
                Timestamp("1997-05-16 00:00:00"): 0.0864579975605011,
                Timestamp("1997-05-19 00:00:00"): 0.0854170024394989,
                Timestamp("1997-05-20 00:00:00"): 0.08177100121974945,
                Timestamp("1997-05-21 00:00:00"): 0.07135400176048279,
            },
            ("Close", "GOOG"): {
                Timestamp("1997-05-15 00:00:00"): float("nan"),
                Timestamp("1997-05-16 00:00:00"): float("nan"),
                Timestamp("1997-05-19 00:00:00"): float("nan"),
                Timestamp("1997-05-20 00:00:00"): float("nan"),
                Timestamp("1997-05-21 00:00:00"): float("nan"),
            },
            ("High", "AMZN"): {
                Timestamp("1997-05-15 00:00:00"): 0.125,
                Timestamp("1997-05-16 00:00:00"): 0.09895800054073334,
                Timestamp("1997-05-19 00:00:00"): 0.08854199945926666,
                Timestamp("1997-05-20 00:00:00"): 0.08749999850988388,
                Timestamp("1997-05-21 00:00:00"): 0.08229199796915054,
            },
            ("High", "GOOG"): {
                Timestamp("1997-05-15 00:00:00"): float("nan"),
                Timestamp("1997-05-16 00:00:00"): float("nan"),
                Timestamp("1997-05-19 00:00:00"): float("nan"),
                Timestamp("1997-05-20 00:00:00"): float("nan"),
                Timestamp("1997-05-21 00:00:00"): float("nan"),
            },
            ("Low", "AMZN"): {
                Timestamp("1997-05-15 00:00:00"): 0.09635400027036667,
                Timestamp("1997-05-16 00:00:00"): 0.0854170024394989,
                Timestamp("1997-05-19 00:00:00"): 0.08124999701976776,
                Timestamp("1997-05-20 00:00:00"): 0.08177100121974945,
                Timestamp("1997-05-21 00:00:00"): 0.06875000149011612,
            },
            ("Low", "GOOG"): {
                Timestamp("1997-05-15 00:00:00"): float("nan"),
                Timestamp("1997-05-16 00:00:00"): float("nan"),
                Timestamp("1997-05-19 00:00:00"): float("nan"),
                Timestamp("1997-05-20 00:00:00"): float("nan"),
                Timestamp("1997-05-21 00:00:00"): float("nan"),
            },
            ("Open", "AMZN"): {
                Timestamp("1997-05-15 00:00:00"): 0.12187500298023224,
                Timestamp("1997-05-16 00:00:00"): 0.09843800216913223,
                Timestamp("1997-05-19 00:00:00"): 0.08802100270986557,
                Timestamp("1997-05-20 00:00:00"): 0.0864579975605011,
                Timestamp("1997-05-21 00:00:00"): 0.08177100121974945,
            },
            ("Open", "GOOG"): {
                Timestamp("1997-05-15 00:00:00"): float("nan"),
                Timestamp("1997-05-16 00:00:00"): float("nan"),
                Timestamp("1997-05-19 00:00:00"): float("nan"),
                Timestamp("1997-05-20 00:00:00"): float("nan"),
                Timestamp("1997-05-21 00:00:00"): float("nan"),
            },
            ("Volume", "AMZN"): {
                Timestamp("1997-05-15 00:00:00"): 1443120000,
                Timestamp("1997-05-16 00:00:00"): 294000000,
                Timestamp("1997-05-19 00:00:00"): 122136000,
                Timestamp("1997-05-20 00:00:00"): 109344000,
                Timestamp("1997-05-21 00:00:00"): 377064000,
            },
            ("Volume", "GOOG"): {
                Timestamp("1997-05-15 00:00:00"): float("nan"),
                Timestamp("1997-05-16 00:00:00"): float("nan"),
                Timestamp("1997-05-19 00:00:00"): float("nan"),
                Timestamp("1997-05-20 00:00:00"): float("nan"),
                Timestamp("1997-05-21 00:00:00"): float("nan"),
            },
        }
    )


@pytest.fixture
def expected() -> DataFrame:
    return DataFrame(
        {
            "symbol": {
                0: "XPTO",
                5: "XPTO",
                10: "XPTO",
                15: "XPTO",
                20: "XPTO",
                25: "XPTO",
                1: "XPTO",
                6: "XPTO",
                11: "XPTO",
                16: "XPTO",
                21: "XPTO",
                26: "XPTO",
                2: "XPTO",
                7: "XPTO",
                12: "XPTO",
                17: "XPTO",
                22: "XPTO",
                27: "XPTO",
                3: "XPTO",
                8: "XPTO",
                13: "XPTO",
                18: "XPTO",
                23: "XPTO",
                28: "XPTO",
                4: "XPTO",
                9: "XPTO",
                14: "XPTO",
                19: "XPTO",
                24: "XPTO",
                29: "XPTO",
            },
            "variable": {
                0: ("Adj Close", "AMZN"),
                5: ("Close", "AMZN"),
                10: ("High", "AMZN"),
                15: ("Low", "AMZN"),
                20: ("Open", "AMZN"),
                25: ("Volume", "AMZN"),
                1: ("Adj Close", "AMZN"),
                6: ("Close", "AMZN"),
                11: ("High", "AMZN"),
                16: ("Low", "AMZN"),
                21: ("Open", "AMZN"),
                26: ("Volume", "AMZN"),
                2: ("Adj Close", "AMZN"),
                7: ("Close", "AMZN"),
                12: ("High", "AMZN"),
                17: ("Low", "AMZN"),
                22: ("Open", "AMZN"),
                27: ("Volume", "AMZN"),
                3: ("Adj Close", "AMZN"),
                8: ("Close", "AMZN"),
                13: ("High", "AMZN"),
                18: ("Low", "AMZN"),
                23: ("Open", "AMZN"),
                28: ("Volume", "AMZN"),
                4: ("Adj Close", "AMZN"),
                9: ("Close", "AMZN"),
                14: ("High", "AMZN"),
                19: ("Low", "AMZN"),
                24: ("Open", "AMZN"),
                29: ("Volume", "AMZN"),
            },
            "value": {
                0: 0.09791699796915054,
                5: 0.09791699796915054,
                10: 0.125,
                15: 0.09635400027036667,
                20: 0.12187500298023224,
                25: 1443120000.0,
                1: 0.0864579975605011,
                6: 0.0864579975605011,
                11: 0.09895800054073334,
                16: 0.0854170024394989,
                21: 0.09843800216913223,
                26: 294000000.0,
                2: 0.0854170024394989,
                7: 0.0854170024394989,
                12: 0.08854199945926666,
                17: 0.08124999701976776,
                22: 0.08802100270986557,
                27: 122136000.0,
                3: 0.08177100121974945,
                8: 0.08177100121974945,
                13: 0.08749999850988388,
                18: 0.08177100121974945,
                23: 0.0864579975605011,
                28: 109344000.0,
                4: 0.07135400176048279,
                9: 0.07135400176048279,
                14: 0.08229199796915054,
                19: 0.06875000149011612,
                24: 0.08177100121974945,
                29: 377064000.0,
            },
            "actual_time": {
                0: 863654400.0,
                5: 863654400.0,
                10: 863654400.0,
                15: 863654400.0,
                20: 863654400.0,
                25: 863654400.0,
                1: 863740800.0,
                6: 863740800.0,
                11: 863740800.0,
                16: 863740800.0,
                21: 863740800.0,
                26: 863740800.0,
                2: 864000000.0,
                7: 864000000.0,
                12: 864000000.0,
                17: 864000000.0,
                22: 864000000.0,
                27: 864000000.0,
                3: 864086400.0,
                8: 864086400.0,
                13: 864086400.0,
                18: 864086400.0,
                23: 864086400.0,
                28: 864086400.0,
                4: 864172800.0,
                9: 864172800.0,
                14: 864172800.0,
                19: 864172800.0,
                24: 864172800.0,
                29: 864172800.0,
            },
        }
    )


def test_get(monkeypatch, data, expected):
    o = YahooTickersSource()
    monkeypatch.setattr(yf, "download", lambda tickers, start, end, interval: data)
    assert_frames_are_equal(actual=o.get(key="XPTO"), expected=expected, sort_columns=["actual_time", "variable"])


def test_get(monkeypatch, data, expected):
    o = YahooTickersSource()
    df = o.get(key="AMD")
    assert 0 < df.size


@pytest.mark.skip(reason="these test should be done manually")
def test_Tickers2S3parquet():
    # fetcher = Tickers2S3parquet()
    # fetcher.fetch(tickers="AMD,NVDA,MSFT", target="s3://de-landing-dev-c7f56307-b37b-4826-a208-edd0ead99c76/dataengineering")

    source = YahooTickersSource()
    df: DataFrame = source.get(key="AMD,NVDA,MSFT")
    assert True
