from typing import Tuple
import pandas as pd
from models.prep_pipeline import PreProcessingStepABC


class LaggedFeatures(PreProcessingStepABC):
    def __init__(self, lags=5, columns=None):
        """
        Custom transformer to add lagged features.

        Parameters:
        - lags: int, number of lags to add for each column.
        - columns: list of column names to add lags for. If None, all columns will be used.
        """
        self.lags = lags
        self.columns = columns

    def transform(self, X, y) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Apply the lagged features transformation to X and y.

        Args:
            X (pd.DataFrame): The input features.
            y (pd.Series): The target variable.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: The transformed features and target variable.
        """
        X = self.transform_X(X)
        y = self.transform_Y(y, X)
        self._validate(X, y)
        return X, y

    def transform_X(self, X):
        """
        Add lagged features to the DataFrame.

        Args:
            X (pd.DataFrame): The input features.

        Returns:
            pd.DataFrame: The DataFrame with lagged features.
        """
        X = X.copy()  # Avoid modifying the original DataFrame
        if self.columns is None:
            self.columns = X.columns.tolist()

        # Create a list to store lagged features
        lagged_features = []

        for col in self.columns:
            for lag in range(1, self.lags + 1):
                lagged_features.append(X[col].shift(lag).rename(f"{col}_lag_{lag}"))

        # Concatenate all the lagged features to the original DataFrame
        X_lagged = pd.concat([X] + lagged_features, axis=1)

        # Drop rows with NaN values introduced by lagging
        X_lagged = X_lagged.dropna()
        return X_lagged

    def transform_Y(self, y, X):
        """
        Transform the target variable y to align with the transformed X.

        Args:
            y (pd.Series): The target variable.
            X (pd.DataFrame): The transformed input features.

        Returns:
            pd.Series: The transformed target variable.
        """
        return y.iloc[self.lags :].reset_index(drop=True)
