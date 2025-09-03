from typing import Optional

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler


class WeatherDataRetriever:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
    ):
        self.client = bigquery.Client()
        self.project_id = project_id
        self.dataset_id = dataset_id

    def get_weather_summary(
        self, table_name: str, limit: Optional[int] = None
    ) -> pd.DataFrame:
        query = f"""
        SELECT 
            date,
            city,
            country,
            avg_temperature,
            max_temperature,
            min_temperature,
            total_precipitation,
            max_wind_speed,
            avg_humidity,
            avg_pressure
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        ORDER BY date DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            df = self.client.query(query).to_dataframe()
            print(f"Retrieved {len(df)} rows of weather data")
            return df
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None

    def prepare_ml_features(
        self,
        df: pd.DataFrame = None,
        target_column: str = "avg_temperature",
        prediction_days: int = 1,
        test_size=0.2,
    ) -> tuple[
        np.array, np.array, np.array, np.array, list[str], StandardScaler, LabelEncoder
    ]:
        """
        Returns:
            tuple: (X_train, X_test, y_train, y_test, feature_names, scaler, label_encoders)
        """
        # Sort by date and city to ensure proper time series ordering
        df = df.sort_values(["city", "date"]).reset_index(drop=True)

        # Create lagged features for time series prediction
        feature_columns = [
            "avg_temperature",
            "max_temperature",
            "min_temperature",
            "total_precipitation",
            "max_wind_speed",
            "avg_humidity",
            "avg_pressure",
        ]

        # Encode categorical variables
        label_encoders = {}
        df_encoded = df.copy()

        for col in ["city", "country"]:
            if col in df.columns:
                le = LabelEncoder()
                df_encoded[f"{col}_encoded"] = le.fit_transform(df[col].astype(str))
                label_encoders[col] = le

        # Create features and targets
        features = []
        targets = []

        for city in df_encoded["city"].unique():
            city_data = df_encoded[df_encoded["city"] == city].sort_values("date")

            if len(city_data) > prediction_days:
                # Create lagged features
                for i in range(prediction_days, len(city_data)):
                    # Features: previous days data
                    feature_row = []

                    # Add lagged weather features
                    for lag in range(1, prediction_days + 1):
                        for col in feature_columns:
                            if col in city_data.columns:
                                feature_row.append(city_data.iloc[i - lag][col])

                    # Add categorical features (city, country)
                    for col in ["city_encoded", "country_encoded"]:
                        if col in city_data.columns:
                            feature_row.append(city_data.iloc[i][col])

                    # Add day of year and month as seasonal features
                    date_val = pd.to_datetime(city_data.iloc[i]["date"])
                    feature_row.extend(
                        [date_val.dayofyear, date_val.month, date_val.weekday()]
                    )

                    features.append(feature_row)
                    targets.append(city_data.iloc[i][target_column])

        # Convert to numpy arrays
        X = np.array(features)
        y = np.array(targets)

        # Create feature names
        feature_names = []
        for lag in range(1, prediction_days + 1):
            for col in feature_columns:
                feature_names.append(f"{col}_lag_{lag}")
        feature_names.extend(
            ["city_encoded", "country_encoded", "day_of_year", "month", "weekday"]
        )

        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, shuffle=False
        )

        # Scale the features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        print(f"Training set size: {X_train_scaled.shape}")
        print(f"Test set size: {X_test_scaled.shape}")

        return (
            X_train_scaled,
            X_test_scaled,
            y_train,
            y_test,
            feature_names,
            scaler,
            label_encoders,
        )


# Usage example
def main():
    # Initialize the data retriever
    retriever = WeatherDataRetriever(
        project_id="dbt-weather-demo", dataset_id="dbt_weather_dataset"
    )

    # Get the weather summary data
    weather_data = retriever.get_weather_summary(
        table_name="weather_summary",
        limit=10000,
    )

    if weather_data is not None:
        print("\nWeather data sample:")
        print(weather_data.head())
        print(f"\nData shape: {weather_data.shape}")
        print(
            f"Date range: {weather_data['date'].min()} to {weather_data['date'].max()}"
        )
        print(f"Cities: {weather_data['city'].nunique()}")


if __name__ == "__main__":
    main()
