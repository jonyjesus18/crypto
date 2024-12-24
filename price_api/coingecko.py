import requests
import os
import pandas as pd
from datetime import datetime
from price_api.api import ApiABC


COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")


class PriceAPI(ApiABC):
    def __init__(self, base_url: str = "api.coingecko.com"):
        super().__init__(base_url)
        self.key = COINGECKO_API_KEY
        self.default_headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": self.key,
        }

    def get_coin_price(self, crypto_id="bitcoin"):
        # Base URL for CoinGecko API
        url = f"https://{self.base_url}/api/v3/simple/price"

        # Build query parameters
        params = {
            "ids": crypto_id,
            "vs_currencies": "usd",
            "include_last_updated_at": "true",
            "precision": "full",
        }

        # Send the GET request
        response = requests.get(url, headers=self.default_headers, params=params)

        # Return the response as JSON
        json_dict_reponse = response.json()

        for coin in json_dict_reponse.keys():
            unix_ts = json_dict_reponse[coin]["last_updated_at"]
            dt_ts = datetime.utcfromtimestamp(unix_ts)
            json_dict_reponse[coin]["last_updated_at"] = dt_ts.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        return json_dict_reponse

    def get_coin_ids(self):
        url = f"https://{self.base_url}/api/v3/coins/list"
        response = requests.get(url, headers=self.default_headers)
        return pd.DataFrame(response.json())

    def get_historical_market_chart_by_id(
        self,
        crypto_id: str = "bitcoin",
        vs_currency: str = "usd",
        days=1,
    ):
        # Base URL for CoinGecko API
        url = f"https://{self.base_url}/api/v3/coins/{crypto_id}/market_chart"

        # Build query parameters
        params = {
            "vs_currency": vs_currency,
            "days": days,
            # "interval": to_timestamp,
        }
        response = requests.get(url, headers=self.default_headers, params=params)
        json_dict_response = response.json()
        return self._process_market_chart_data(json_dict_response)

    def get_historical_market_chart_range(
        self,
        crypto_id: str = "bitcoin",
        vs_currency: str = "usd",
        from_date: str = "2024-01-01",
        to_date: str = "2024-01-02",
    ) -> pd.DataFrame:
        # Convert date strings to UNIX timestamps
        from_timestamp, to_timestamp = self._timestamp_to_unix(from_date, to_date)

        # Base URL for CoinGecko API
        url = f"https://{self.base_url}/api/v3/coins/{crypto_id}/market_chart/range"

        # Build query parameters
        params = {
            "vs_currency": vs_currency,
            "from": from_timestamp,
            "to": to_timestamp,
        }
        response = requests.get(url, headers=self.default_headers, params=params)
        if response.status_code != 200:
            raise Exception(
                f"Error fetching data: {response.status_code} - {response.text}"
            )
        json_dict_response = response.json()
        return self._process_market_chart_data(json_dict_response)

    @staticmethod
    def _timestamp_to_unix(start_date: str, end_date: str):
        from_timestamp_unix = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        to_timestamp_unix = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        return from_timestamp_unix, to_timestamp_unix

    @staticmethod
    def _process_market_chart_data(json: dict):
        prices = json["prices"]
        market_caps = json["market_caps"]
        total_volumes = json["total_volumes"]

        df_bulk = pd.DataFrame()
        for df, series in [
            (prices, "price"),
            (market_caps, "market_caps"),
            (total_volumes, "total_volumes"),
        ]:
            df = pd.DataFrame(prices)
            df.columns = ["unix_ts", "price"]  # type: ignore
            df["series"] = series
            df_bulk = pd.concat([df_bulk, df])

        df_bulk["datetime"] = pd.to_datetime(df_bulk["unix_ts"], unit="ms", utc=True)
        return df_bulk
