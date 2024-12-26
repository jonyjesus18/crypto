import pandas as pd
from dotenv import load_dotenv
from loguru import logger
import pandas as pd
from db.mongodb import MongoDB
from price_api.coingecko import PriceAPI
from price_api.tiingo import TiingoAPI
from utils.nomenclature import TimeSeriesFields, Source

from utils.py_utils import index_slice

pd.options.plotting.backend = "plotly"


date_range = pd.date_range(start="2023-01-01", end="2024-12-31", freq="15D")
date_range = list(zip(date_range[:-1], date_range[1:]))

for start_date, end_date in date_range:
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    logger.info(f"Processing {start_date_str} to {end_date_str}")

    prices: pd.DataFrame = TiingoAPI().get_prices(
        tickers="btcusd", start_date=start_date_str, end_date=end_date_str
    )
    prices_df_melt = prices.melt(
        id_vars=["date", "ticker"], var_name="field", value_name="value"
    )
    prices_df_melt[TimeSeriesFields.SOURCE] = Source.TIINGO
    prices_df_melt = prices_df_melt.rename(columns={"date": TimeSeriesFields.DATETIME})

    MongoDB().save_dataframe_to_collection(
        dataframe=prices_df_melt, database="prices", collection="coin_timeseries"
    )
