from data_hooks.tiingo import Tiingo
from utils.py_utils import collapse_multi_index_cols, index_slice
import pandas as pd
import numpy as np
from typing import Tuple


class MomentumModelData:
    def __init__(self, n_steps: int = 12, threshold: float = 0.01):
        self.n_steps = n_steps
        self.threshold = threshold

    def prepare_data(
        self, start_date: str, end_date: str | None = None
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare X and y data for modeling."""
        X = self.get_x_data(start_date, end_date)
        y = self.get_y_data(start_date, end_date)

        # Align indices
        aligned_idx = X.index.intersection(y.index)
        return X.loc[aligned_idx], y.loc[aligned_idx]

    def get_x_data(self, start_date: str, end_date: str | None = None) -> pd.DataFrame:
        """Generate feature matrix X."""
        df = Tiingo().get_data(start_date=start_date, end_date=end_date, cache=True)
        close = collapse_multi_index_cols(
            index_slice(df, field="close", ticker="btcusd")
        )
        close = close.squeeze()  # Convert DataFrame to Series

        X = pd.DataFrame(index=close.index)

        # Technical features
        X["rsi"] = self._calculate_rsi(close)
        X["momentum"] = close.pct_change(12)
        X["sma_cross"] = (close > close.rolling(20).mean()).astype(int)

        # Return features
        for period in [12, 24, 24 * 7]:
            X[f"returns_{period}"] = close.pct_change(period)
            X[f"volatility_{period}"] = close.pct_change().rolling(period).std()

        return X.dropna()

    def get_y_data(self, start_date: str, end_date: str | None = None) -> pd.Series:
        """Generate target variable."""
        df = Tiingo().get_data(start_date=start_date, end_date=end_date, cache=True)
        close = collapse_multi_index_cols(
            index_slice(df, field="close", ticker="btcusd")
        )
        close = close.squeeze()  # Convert DataFrame to Series

        future_returns = close.shift(-self.n_steps).div(close) - 1
        y = pd.Series(0, index=close.index)
        y.loc[future_returns > self.threshold] = 1  # Use .loc for boolean indexing
        return y[: -self.n_steps]

    def _calculate_rsi(self, price: pd.Series, periods: int = 14) -> pd.Series:
        """Calculate RSI indicator."""
        delta = price.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()  # type: ignore
        loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()  # type: ignore
        rs = gain / loss
        return 100 - (100 / (1 + rs))
