import jwt
from cryptography.hazmat.primitives import serialization
import time
import secrets
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()


class CoinbaseBaseAPI:
    def __init__(self):
        self.key_name = os.getenv("COINBASE_API_KEY")
        self.key_secret = os.getenv("COINBASE_API_SECRET")
        self.request_host = "api.coinbase.com"

    def _build_jwt(self, uri):
        private_key_bytes = self.key_secret.encode("utf-8")  # type: ignore
        private_key = serialization.load_pem_private_key(
            private_key_bytes, password=None
        )
        jwt_payload = {
            "sub": self.key_name,
            "iss": "cdp",
            "nbf": int(time.time()),
            "exp": int(time.time()) + 120,
            "uri": uri,
        }
        jwt_token = jwt.encode(
            jwt_payload,
            private_key,  # type: ignore
            algorithm="ES256",
            headers={"kid": self.key_name, "nonce": secrets.token_hex()},
        )
        return jwt_token

    def make_request(self):
        # Prepare the URI
        path = "/api/v3/brokerage/accounts"
        uri = f"GET {self.request_host}{path}"
        jwt_token = self._build_jwt(uri)

        # Set the request headers with the JWT token
        headers = {
            "Authorization": f"Bearer {jwt_token}",
        }

        # Make the GET request to Coinbase API
        url = f"https://{self.request_host}{path}"
        response = requests.get(url, headers=headers)

        # Check the response status and print the result
        if response.status_code == 200:
            print("Account Details:", response.json())
        else:
            print(f"Error: {response.status_code} - {response.text}")

    def make_post_request(self, path, payload):
        # Prepare the URI
        uri = f"POST {self.request_host}{path}"
        jwt_token = self._build_jwt(uri)

        # Set the request headers with the JWT token
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
        }

        # Make the POST request to Coinbase API
        url = f"https://{self.request_host}{path}"
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # Check the response status and print the result
        if response.status_code == 200:
            print("Order Preview:", response.json())
        else:
            print(f"Error: {response.status_code} - {response.text}")
