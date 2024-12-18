import requests
import os
from datetime import datetime
from price_api.api import ApiABC
from dotenv import load_dotenv

load_dotenv()

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")


class PriceAPI(ApiABC):
    def __init__(self, base_url: str = "api.coingecko.com"):
        super().__init__(base_url)
        self.key = COINGECKO_API_KEY
        self.default_headers = headers = {
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
        response
        # Return the response as JSON
        json_dict_reponse = response.json()

        for coin in json_dict_reponse.keys():
            unix_ts = json_dict_reponse[coin]["last_updated_at"]
            dt_ts = datetime.utcfromtimestamp(unix_ts)
            json_dict_reponse[coin]["last_updated_at"] = dt_ts.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        return json_dict_reponse
