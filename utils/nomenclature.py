from utils.py_utils import StrValueEnum


class TimeSeriesFields(StrValueEnum):
    DATETIME = "datetime"
    FIELD = "field"
    VALUE = "value"
    SOURCE = "source"


class Source(StrValueEnum):
    TIINGO = "tiingo"
    COINGECKO = "coingecko"
    COINBASE = "coinbase"


class Coin(StrValueEnum):
    BTC_USD = "btc_usd"
