import click
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from mongodb import MongoDB
from price_api.tiingo import TiingoAPI
from utils.nomenclature import TimeSeriesFields, Source

# Load environment variables
load_dotenv()


@click.command()
@click.option(
    "--ticker", required=True, help="The ticker symbol for the coin (e.g., btcusd)."
)
@click.option(
    "--start-date", required=True, help="The start date in YYYY-MM-DD format."
)
@click.option("--end-date", required=True, help="The end date in YYYY-MM-DD format.")
def fetch_and_store_prices(ticker, start_date, end_date):
    """
    Fetches price data for the given ticker and date range, and stores it in MongoDB.
    """
    try:
        # Generate date ranges with 15-day intervals
        date_range = pd.date_range(start=start_date, end=end_date, freq="15D")
        date_range = list(zip(date_range[:-1], date_range[1:]))

        for start, end in date_range:
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

            # Save to MongoDB
            MongoDB().save_dataframe_to_collection(
                dataframe=prices_df_melt,
                database="prices",
                collection="coin_timeseries",
            )

        logger.info("All data processed and saved successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    fetch_and_store_prices()

# PYTHONPATH=$(pwd) python db/load_tiingo.py --ticker btcusd --start-date 2024-05-01 --end-date 2024-12-31
