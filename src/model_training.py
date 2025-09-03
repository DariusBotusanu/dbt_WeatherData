import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

import pickle
from typing import Optional

import mlflow
import numpy as np
import optuna
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

from weather_ml_retrieval import WeatherDataRetriever


def load_data(retriever: Optional[WeatherDataRetriever] = None) -> pd.DataFrame:
    if retriever is None:
        retriever = WeatherDataRetriever(
            project_id="dbt-weather-demo", dataset_id="dbt_weather_dataset"
        )

    weather_data = retriever.get_weather_summary(
        table_name="weather_summary",
        limit=10000,
    )

    return weather_data


class RFHyperparameterTuning:
    def __init__(self, X_train, X_test, y_train, y_test):
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test

        mlflow.set_experiment("temperature_prediction")

    def objective(self, trial):
        n_estimators = trial.suggest_int("n_estimators", 10, 100)
        max_depth = trial.suggest_int("max_depth", 2, 10)

        with mlflow.start_run():
            model = RandomForestRegressor(
                n_estimators=n_estimators, max_depth=max_depth
            )
            model.fit(self.X_train, self.y_train)
            y_pred = model.predict(self.X_test)

            signature = mlflow.infer_signature(self.X_test, y_pred)

            mse = mean_squared_error(self.y_test, y_pred)
            r2 = r2_score(self.y_test, y_pred)

            # Log parameters and accuracy
            mlflow.log_param("n_estimators", n_estimators)
            mlflow.log_param("max_depth", max_depth)

            mse = mean_squared_error(self.y_test, y_pred)
            r2 = r2_score(self.y_test, y_pred)

            mlflow.log_metric("MSE", mse)
            mlflow.log_metric("RMSE", np.sqrt(mse))
            mlflow.log_metric("r2", r2)

            return mse

    def tune_hyperparamters(self) -> dict:
        """
        Find the best hyperparameters and save the model
        """
        study = optuna.create_study(direction="minimize")
        study.optimize(self.objective, n_trials=20)

        params = study.best_params

        return params

    def save_model(self, params: dict):
        model = RandomForestRegressor(**params)
        model.fit(self.X_train, self.y_train)
        model_params = model.get_params()

        mlflow.log_params(params)

        # save the model
        model_path = os.path.join(PROJECT_ROOT, "src", "saved_models", "model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        mlflow.sklearn.log_model(sk_model=model, artifact_path=model_path)

        return model


if __name__ == "__main__":
    retriever = WeatherDataRetriever(
        project_id="dbt-weather-demo", dataset_id="dbt_weather_dataset"
    )
    df = load_data(retriever)
    # Prepare data for ML
    X_train, X_test, y_train, y_test, _, _, _ = retriever.prepare_ml_features(
        df,
        target_column="avg_temperature",  # target
        prediction_days=1,  # forecast length
        test_size=0.2,
    )

    rf_tuner = RFHyperparameterTuning(X_train, X_test, y_train, y_test)

    best_params = rf_tuner.tune_hyperparamters()

    model = rf_tuner.save_model(best_params)
