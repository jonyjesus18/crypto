import click
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from db.timescaledb import TimescaleDB
from price_api.tiingo import TiingoAPI
from utils.nomenclature import TimeSeriesFields, Source


def create_date_ranges(start_date: str, end_date: str, freq: str = "15D") -> list:
    """Create date ranges ensuring full coverage including end date"""
    # Convert to datetime
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # Generate ranges
    dates = pd.date_range(start=start, end=end, freq=freq)
    if dates[-1] < end:
        dates = dates.append(pd.DatetimeIndex([end]))

    # Create pairs
    return list(zip(dates[:-1], dates[1:]))


start_date = "2023-06-01"
end_date = "2024-01-01"
ticker = "btcusd"

# Generate date ranges with validation
date_range = create_date_ranges(start_date, end_date)
logger.info(f"Created {len(date_range)} date ranges")
logger.info(f"First range: {date_range[0][0]} to {date_range[0][1]}")
logger.info(f"Last range: {date_range[-1][0]} to {date_range[-1][1]}")

upload_df = pd.DataFrame()
for start, end in date_range:
    try:
        start_date_str = start.strftime("%Y-%m-%d")
        end_date_str = end.strftime("%Y-%m-%d")
        logger.info(f"Processing {start_date_str} to {end_date_str} for {ticker}")

        # Fetch prices from TiingoAPI
        prices: pd.DataFrame = TiingoAPI().get_prices(
            tickers=ticker, start_date=start_date_str, end_date=end_date_str
        )

        # Transform the data
        prices_df_melt = prices.melt(
            id_vars=["date", "ticker"], var_name="field", value_name="value"
        )
        prices_df_melt[TimeSeriesFields.SOURCE] = Source.TIINGO
        prices_df_melt = prices_df_melt.rename(
            columns={"date": TimeSeriesFields.DATETIME}
        )
        prices_df_melt["datetime"] = pd.to_datetime(
            prices_df_melt["datetime"]
        ).dt.tz_localize(None)
        upload_df = pd.concat([upload_df, prices_df_melt])
    except Exception as e:
        logger.error(e)
        continue

# Validate complete date coverage
date_range = pd.date_range(start=start_date, end=end_date, freq="1D")
missing_dates = set(date_range) - set(upload_df["datetime"].dt.normalize().unique())
if missing_dates:
    logger.warning(f"Missing dates: {sorted(missing_dates)}")


await TimescaleDB().copy_dataframe_to_table(
    df=upload_df,
    table_name="bars",
    columns=["datetime", "ticker", "field", "value", "source"],
)
