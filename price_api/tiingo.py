import requests
import pandas as pd
from price_api.api import ApiABC


class TiingoAPI(ApiABC):
    def __init__(self):
        self.base_url = "https://api.tiingo.com/tiingo/crypto/"
        self.key = self._get_os_key("TIINGO_API_TOKEN")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.key}",
        }

    def metadata(
        self,
        tickers: str | list[str] | None = "btcusd",
    ):
        """
        Fetches price data for the given parameters from Tiingo API.

        Args:
            tickers (str): Comma-separated string of tickers (e.g., "btcusd,ethusd").
            start_date (str): Start date for the data in YYYY-MM-DD format.
            end_date (str): End date for the data in YYYY-MM-DD format.
            resample_freq (str): Frequency for resampling data (e.g., "5min", "1hour").

        Returns:
            dict: JSON response containing price data.

        Example:
            TiingoAPI().get_prices(
                tickers='btcusd',
                start_date='2024-01-01',
                end_date='2024-02-01',
                resample_freq='5min',
                exchangeData='true',
            )
        """

        if tickers is not None:
            tickers = tickers if isinstance(tickers, str) else ",".join(tickers)
            params = {
                "tickers": tickers,
            }
        else:
            params = None
        response = requests.get(self.base_url, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_prices(
        self,
        tickers: str,
        start_date: str,
        end_date: str,
        resample_freq: str = "5min",
        exchangeData: str = "true",
        return_as_df: bool = True,
    ):
        """
        Fetches price data for the given parameters from Tiingo API.

        Args:
            tickers (str): Comma-separated string of tickers (e.g., "btcusd,ethusd").
            start_date (str): Start date for the data in YYYY-MM-DD format.
            end_date (str): End date for the data in YYYY-MM-DD format.
            resample_freq (str): Frequency for resampling data (e.g., "5min", "1hour").

        Returns:
            dict: JSON response containing price data.

        Example:
            TiingoAPI().get_prices(
                tickers='btcusd',
                start_date='2024-01-01',
                end_date='2024-02-01',
                resample_freq='5min',
                exchangeData='true',
            )
        """

        params = {
            "tickers": tickers,
            "startDate": start_date,
            "endDate": end_date,
            "resampleFreq": resample_freq,
            "exchangeData": exchangeData,
        }
        response = requests.get(
            self.base_url + "prices", headers=self.headers, params=params
        )
        if response.status_code == 200:
            data_out = response.json()[0]
            if return_as_df:
                ticker = data_out["ticker"]
                data = data_out["priceData"]
                df = pd.DataFrame(data)
                df["ticker"] = ticker
                return df
            return data_out
        else:
            raise Exception(response.json())
