# The custom PreProcessingPipeline class
from loguru import logger
from typing import List, Tuple
from abc import ABC
import pandas as pd


class PreProcessingStepABC(ABC):
    def __init__(self):
        pass

    def transform(self, X, y) -> Tuple:
        """
        Apply transformations to both X and y.

        Returns:
        - Transformed X and y.
        """
        X = self.transform_X(X)
        y = self.transform_Y(y)
        self._validate(X, y)

        return X, y

    def transform_X(self, X, y=None):
        """
        Add lagged features to the DataFrame (example).
        Override this method in subclasses to define specific transformations for X.
        """
        return X  # Default: no transformation

    def transform_Y(self, y, X=None):
        """
        Transform the target variable.
        Override this method in subclasses to define specific transformations for y.
        """
        return y  # Default: no transformation

    def _validate(self, X, y):
        """
        Validate that X and y have no NaN values and are the same size.
        """
        if X.isnull().any().any():
            raise ValueError("X contains NaN values.")
        if y.isnull().any():
            raise ValueError("y contains NaN values.")
        if len(X) != len(y):
            raise ValueError(
                f"X and y must have the same number of rows. X has {len(X)} rows, y has {len(y)} rows."
            )


class PreProcessingPipeline:
    def __init__(self, steps: List[PreProcessingStepABC]):
        """
        Initialize the preprocessing pipeline with a list of steps.

        Args:
            steps (List[Type[PreProcessingStepABC]]): List of preprocessing step objects.
        """
        self.steps = steps

    def transform(self, X, y) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply all preprocessing steps to both X and y.

        Args:
            X (pd.DataFrame): The input features.
            y (pd.Series): The target variable.

        Returns:
            Tuple[pd.DataFrame, pd.Series]: The transformed features and target variable.
        """
        for i, step in enumerate(self.steps):
            logger.info(
                f"Applying preprocessing step {i + 1}: {step.__class__.__name__} of {len(self.steps)}"
            )
            X, y = step.transform(X, y)
        return X, y
