# app.py
import time
from price_api.coingecko import PriceAPI
from flask import Flask
import threading

price_api = PriceAPI(base_url="api.coingecko.com")

app = Flask(__name__)


def print_hello():
    while True:
        print(price_api.get_coin_price("bitcoin"))
        time.sleep(30)


@app.route("/")
def index():
    return "Hello! The app is running. Check the console for periodic 'hello' messages."


# Start the background thread for printing "hello"
thread = threading.Thread(target=print_hello, daemon=True)
thread.start()
