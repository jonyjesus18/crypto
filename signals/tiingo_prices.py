from functools import lru_cache
from data_hooks.data_hook import Datahook
from data_hooks.tiingo import Tiingo
from utils.py_utils import keep_levels
from loguru import logger
import pandas as pd


class TiingoPriceSignal(Datahook):
    @lru_cache
    def get_data(self):
        df = Tiingo().get_data(start_date="2024-01-01")
        df = keep_levels(df, levels_to_keep=["field"])
        df = df.resample("h").last()
        self.df = df
        return df

    @lru_cache
    def get_y_data(self, future_window=24, threshold=0.01, **kwargs):
        """
        Create the target variable: 1 if price increases by the threshold in the future_window, else 0.
        Parameters:
        - df: DataFrame with 5m bar data.
        - future_window: Number of future bars to look ahead.
        - threshold: Minimum percentage increase in the close price to classify as "buy".
        """
        future_returns = self.df["close"].shift(-future_window) / self.df["close"] - 1
        target = (future_returns > threshold).astype(int)
        return target

    @lru_cache
    def get_x_data(self, window=12, **kwargs):
        """
        Create features for the model using momentum indicators and recent data.
        Parameters:
        - df: DataFrame with 5m bar data.
        - window: Number of datapoints (bars) to use for feature creation.
        """
        features = pd.DataFrame(index=self.df.index)

        # Moving averages
        features["ma_20"] = self.df["close"].rolling(20).mean()
        features["ma_50"] = self.df["close"].rolling(50).mean()

        # Rate of change (momentum)
        features["roc"] = self.df["close"].pct_change(window)

        # Relative Strength Index (RSI)
        delta = self.df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window).mean()  # type: ignore
        loss = (-delta.where(delta < 0, 0)).rolling(window).mean()  # type: ignore
        rs = gain / loss
        features["rsi"] = 100 - (100 / (1 + rs))
        features["rsi_diff"] = features.rsi.diff().rolling(window).mean()
        # Price range (high-low)
        # features["range"] = df["high"] - df["low"]
        features["close"] = self.df["close"]

        # Volume-based features
        features["volume_mean"] = self.df["volume"].rolling(window).mean()
        features["trades_mean"] = self.df["tradesDone"].rolling(window).mean()

        return features[["rsi_diff"]]

    def get_x_y_data(self, x_kwargs={}, y_kwargs={}):
        logger.info("Loading data...")
        self.get_data()
        logger.info("Data loaded.")
        logger.info("Creating features and target variable...")
        y = self.get_y_data(**y_kwargs)
        X = self.get_x_data(**x_kwargs).dropna()
        y = y.loc[X.index]

        return X, y
