from functools import lru_cache
from data_hooks.data_hook import Datahook
from data_hooks.tiingo import Tiingo
from utils.py_utils import keep_levels
import pandas as pd


class TiingoPriceSignal(Datahook):
    @lru_cache
    def get_data(self):
        df = Tiingo().get_data(start_date="2023-01-01")
        df = keep_levels(df, levels_to_keep=["field"])
        return df

    def get_y_data(self, df, future_window=60, threshold=0.02, **kwargs):
        """
        Create the target variable: 1 if price increases by the threshold in the future_window, else 0.
        Parameters:
        - df: DataFrame with 5m bar data.
        - future_window: Number of future bars to look ahead.
        - threshold: Minimum percentage increase in the close price to classify as "buy".
        """
        future_returns = df["close"].shift(-future_window) / df["close"] - 1
        target = (future_returns > threshold).astype(int)
        return target

    def get_x_data(self, df, window=60, **kwargs):
        """
        Create features for the model using momentum indicators and recent data.
        Parameters:
        - df: DataFrame with 5m bar data.
        - window: Number of datapoints (bars) to use for feature creation.
        """
        features = pd.DataFrame(index=df.index)

        # Moving averages
        features["ma_20"] = df["close"].rolling(20).mean()
        features["ma_50"] = df["close"].rolling(50).mean()

        # Rate of change (momentum)
        features["roc"] = df["close"].pct_change(window)

        # Relative Strength Index (RSI)
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window).mean()  # type: ignore
        loss = (-delta.where(delta < 0, 0)).rolling(window).mean()  # type: ignore
        rs = gain / loss
        features["rsi"] = 100 - (100 / (1 + rs))

        # Price range (high-low)
        features["range"] = df["high"] - df["low"]
        features["close"] = df["close"]

        # Volume-based features
        features["volume_mean"] = df["volume"].rolling(window).mean()
        features["volume_change"] = df["volume"].pct_change(window)
        features["trades_mean"] = df["tradesDone"].rolling(window).mean()
        features["trades_change"] = df["tradesDone"].pct_change(window)
        return features

    def get_x_y_data(self, x_kwargs={}, y_kwargs={}):
        df = self.get_data()
        y = self.get_y_data(df, **y_kwargs)
        X = self.get_x_data(df, **x_kwargs).dropna()
        y = y.loc[X.index]

        return X, y
