from coinbase_api.base_client import CoinbaseBaseAPI


class CoinbaseAPI(CoinbaseBaseAPI):
    def preview_order(
        self,
        product_id: str = "BTC-USDC",
        order_configuration: dict = {"market_market_ioc": {"quote_size": "1"}},
        side: str = "BUY",
    ):
        payload = {
            "product_id": product_id,
            "order_configuration": order_configuration,
            "side": side,
        }

        self.make_post_request("/api/v3/brokerage/orders/preview", payload)

    def create_order(
        self,
        product_id: str = "BTC-USD",
        order_configuration: dict = {
            "limit_limit_gtc": {
                "quote_size": "1",
                "limit_price": "20",
                "post_only": False,
            },
        },
        side: str = "BUY",
        client_order_id: str = "test_1",
    ):
        # Construct the payload
        payload = {
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": side,
            "order_configuration": order_configuration,
        }

        # Call the POST request method
        self.make_post_request("/api/v3/brokerage/orders", payload)
