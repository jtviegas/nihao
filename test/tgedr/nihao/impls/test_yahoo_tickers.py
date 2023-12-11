import pytest
import yfinance as yf
from pandas import DataFrame, Timestamp
from test.conftest import assert_frames_are_equal

from tgedr.nihao.impls.sources.yahoo_tickers import YahooTickersSource


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
                0: "AMZN",
                5: "AMZN",
                10: "AMZN",
                15: "AMZN",
                20: "AMZN",
                25: "AMZN",
                1: "AMZN",
                6: "AMZN",
                11: "AMZN",
                16: "AMZN",
                21: "AMZN",
                26: "AMZN",
                2: "AMZN",
                7: "AMZN",
                12: "AMZN",
                17: "AMZN",
                22: "AMZN",
                27: "AMZN",
                3: "AMZN",
                8: "AMZN",
                13: "AMZN",
                18: "AMZN",
                23: "AMZN",
                28: "AMZN",
                4: "AMZN",
                9: "AMZN",
                14: "AMZN",
                19: "AMZN",
                24: "AMZN",
                29: "AMZN",
            },
            "variable": {
                0: "Adj Close",
                5: "Close",
                10: "High",
                15: "Low",
                20: "Open",
                25: "Volume",
                1: "Adj Close",
                6: "Close",
                11: "High",
                16: "Low",
                21: "Open",
                26: "Volume",
                2: "Adj Close",
                7: "Close",
                12: "High",
                17: "Low",
                22: "Open",
                27: "Volume",
                3: "Adj Close",
                8: "Close",
                13: "High",
                18: "Low",
                23: "Open",
                28: "Volume",
                4: "Adj Close",
                9: "Close",
                14: "High",
                19: "Low",
                24: "Open",
                29: "Volume",
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
    actual = o.get(key="GOOG,AMZN")
    assert_frames_are_equal(actual=actual, expected=expected, sort_columns=["actual_time", "variable"])


# @pytest.mark.skip(reason="these test should be done manually")
def test_Tickers2S3parquet():
    # fetcher = Tickers2S3parquet()
    # fetcher.fetch(tickers="AMD,NVDA,MSFT", target="s3://de-landing-dev-c7f56307-b37b-4826-a208-edd0ead99c76/dataengineering")

    source = YahooTickersSource()
    df: DataFrame = source.get(key="AMD,NVDA,MSFT")
    assert True
