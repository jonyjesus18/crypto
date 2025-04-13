from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import r2_score, mean_squared_error, classification_report
import pandas as pd
import numpy as np
from typing import Dict
from models.model_data import MomentumModelData


class MomentumModel:
    def __init__(self, n_steps: int = 12, threshold: float = 0.01):
        self.data = MomentumModelData(n_steps=n_steps, threshold=threshold)
        self.pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("classifier", LogisticRegression(random_state=42)),
            ]
        )
        self.tscv = TimeSeriesSplit(n_splits=5, test_size=24 * 7)
        self._fitted_model = None

    def get_fitted_model(self, start_date: str, end_date: str | None = None):
        """Get or create fitted model."""
        if self._fitted_model is None:
            X, y = self.data.prepare_data(start_date, end_date)
            self.pipeline.fit(X, y)
            self._fitted_model = self.pipeline
        return self._fitted_model

    def predict(self, start_date: str, end_date: str | None = None) -> pd.Series:
        """Generate predictions using fitted model."""
        if self._fitted_model is None:
            self._fitted_model = self.get_fitted_model(start_date, end_date)

        X = self.data.get_x_data(start_date, end_date)
        predictions = self._fitted_model.predict(X)

        return pd.Series(predictions, index=X.index)

    def evaluate(self, start_date: str, end_date: str | None = None) -> Dict:
        """Evaluate model using time series cross-validation."""
        X, y = self.data.prepare_data(start_date, end_date)

        metrics = {"r2_scores": [], "rmse_scores": [], "classification_reports": []}

        for train_idx, test_idx in self.tscv.split(X):
            X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

            self.pipeline.fit(X_train, y_train)
            y_pred = self.pipeline.predict(X_test)

            metrics["r2_scores"].append(r2_score(y_test, y_pred))
            metrics["rmse_scores"].append(np.sqrt(mean_squared_error(y_test, y_pred)))
            metrics["classification_reports"].append(
                classification_report(y_test, y_pred)
            )

        return metrics
