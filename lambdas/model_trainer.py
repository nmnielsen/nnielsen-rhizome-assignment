# from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import pandas as pd


class ModelTrainer:
    def __init__(self, model_type="xgboost", model_params=None):
        """
        Initializes the ModelRunner with the specified model type and parameters.

        Parameters:
        - model_type: str, either "xgboost" or "random_forest"
        - model_params: dict, parameters for the model constructor
        """
        self.model_type = model_type
        self.model_params = model_params or {}

    def train_and_evaluate(self, df, target_col, test_split_date, features=None):
        """
        Trains and evaluates the specified model on the given dataset.

        Parameters:
        - df: pandas DataFrame with datetime index and features
        - target_col: string, name of the target column
        - test_split_date: ISO string for split point (index must be datetime)
        - features: list of feature columns to use (defaults to all except target)

        Returns:
        - model_params: dict, parameters of the trained model
        - results_df: pandas DataFrame with y_test and y_pred columns
        - metrics: dict, evaluation metrics (RMSE, MAE, R²)
        """
        train_df = df[df.index < test_split_date]
        test_df = df[df.index >= test_split_date]

        if features is None:
            features = [c for c in df.columns if c != target_col]

        X_train, y_train = train_df[features], train_df[target_col]
        X_test, y_test = test_df[features], test_df[target_col]

        # Initialize the model
        if self.model_type == "xgboost":
            raise NotImplementedError("XGBoost model training is not implemented in this example.")
            # model = XGBRegressor(**self.model_params)
        elif self.model_type == "random_forest":
            model = RandomForestRegressor(**self.model_params)
        else:
            raise ValueError("Invalid model_type. Choose 'xgboost' or 'random_forest'.")

        # Train the model
        model.fit(X_train, y_train)

        # Make predictions
        y_pred = model.predict(X_test)

        # Calculate metrics
        metrics = {
            "RMSE": mean_squared_error(y_test, y_pred),
            "MAE": mean_absolute_error(y_test, y_pred),
            "R2": r2_score(y_test, y_pred)
        }

        prediction_results_df = pd.DataFrame({
            "y_test": y_test,
            "y_pred": y_pred
        }, index=test_df.index)

        return model, prediction_results_df, metrics
