import requests
from price_api.api import ApiABC


class TiingoAPI(ApiABC):
    def __init__(self):
        self.base_url = "https://api.tiingo.com/tiingo/crypto/prices"
        self.key = self._get_os_key("TIINGO_API_TOKEN")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.key}",
        }

    def get_prices(self, tickers, start_date, end_date, resample_freq):
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
                resample_freq='5min'
            )
        """

        params = {
            "tickers": tickers,
            "startDate": start_date,
            "endDate": end_date,
            "resampleFreq": resample_freq,
        }
        response = requests.get(self.base_url, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
