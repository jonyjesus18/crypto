from sklearn.pipeline import Pipeline
from models.prep_pipeline import PreProcessingPipeline


class ModelPipeline(Pipeline):
    def __init__(
        self, pre_process_pipeline: PreProcessingPipeline, steps, verbose=True
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
        X_processed, y_processed = self.pre_process_pipeline.transform(X, y)

        # Fit the model pipeline with the processed data
        return super().fit(X_processed, y_processed)

    def predict(self, X, y):
        """
        First apply the pre-processing pipeline to X, then make predictions using the model.
        """
        # Preprocess the data using the preprocessing pipeline
        X_processed, _ = self.pre_process_pipeline.transform(X, y)

        # Make predictions using the model pipeline
        return super().predict(X_processed)
