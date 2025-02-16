from sklearn.pipeline import Pipeline
import pandas as pd
from models.prep_pipeline import PreProcessingPipeline


class ModelPipeline(Pipeline):
    def __init__(
        self,
        steps,
        pre_process_pipeline: PreProcessingPipeline | None = None,
        verbose=True,
    ):
        """
        Initialize the pipeline with a preprocessing pipeline and a regular model pipeline.

        Parameters:
        - pre_process_pipeline: PreProcessingPipeline object to preprocess the data.
        - steps: List of steps for the regular sklearn pipeline (e.g., model, scaler, etc.)
        """
        self.pre_process_pipeline = pre_process_pipeline
        super().__init__(steps, verbose=verbose)

    def fit(self, X, y):
        """
        First apply the pre-processing pipeline to X and y, then fit the model.
        """
        # Preprocess the data using the preprocessing pipeline
        if self.pre_process_pipeline is not None:
            X, y = self.pre_process_pipeline.transform(X, y)

        # Fit the model pipeline with the processed data
        return super().fit(X, y)

    def predict(self, X: pd.DataFrame, y: pd.DataFrame, pred_col_name="prediction"):
        """
        First apply the pre-processing pipeline to X, then make predictions using the model.
        """
        # Preprocess the data using the preprocessing pipeline

        if self.pre_process_pipeline is not None:
            X, y = self.pre_process_pipeline.transform(X.copy(), y.copy())

        # Make predictions using the model pipeline
        prediction = super().predict(X)
        return pd.DataFrame(prediction, columns=[pred_col_name], index=X.index)
