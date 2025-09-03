import datetime
from datetime import timedelta
from typing import Optional, Union

import polars as pl
import requests
from google.cloud import bigquery


def fetch_historical_weather_data(
    end_date: Union[str, datetime.datetime.date] = datetime.datetime.now().date(),
    days_back=30,
    city: str = "Cluj-Napoca",
    country: str = "Romania",
    latitude: float = 46.7712,
    longitude: float = 23.6236,
    timezone: str = "Europe/Bucharest",
) -> Optional[dict]:
    # Convert end_date to datetime object if it's a string
    if isinstance(end_date, str):
        try:
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid date format. Use 'YYYY-MM-DD'")
            return None

    # Calculate start date
    start_date = end_date - timedelta(days=days_back)

    # Ensure we don't request future data
    today = datetime.datetime.now().date()
    if end_date > today:
        print(f"End date {end_date} is in the future. Using today's date instead.")
        end_date = datetime.combine(today, datetime.min.time())

    url = "https://archive-api.open-meteo.com/v1/archive"

    # Coordinates for Cluj-Napoca
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": [
            "temperature_2m",
            "precipitation",
            "relative_humidity_2m",
            "wind_speed_10m",
            "pressure_msl",
        ],
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
        ],
        "timezone": timezone,
    }

    try:
        print(f"Fetching historical data from {start_date} to {end_date}...")
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        # Structure the historical data
        historical_data = {
            "location": {
                "city": city,
                "country": country,
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "timezone": data.get("timezone"),
            },
            "period": {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "total_days": days_back,
            },
            "hourly_data": data.get("hourly", {}),
            "daily_data": data.get("daily", {}),
        }

        return historical_data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching historical weather data: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing JSON response: {e}")
        return None


def flatten_hourly(hourly_data: dict, location: dict):
    df = pl.DataFrame(hourly_data)
    df = df.with_columns(
        [
            pl.lit(location["city"]).alias("city"),
            pl.lit(location["country"]).alias("country"),
        ]
    )
    return df


def flatten_daily(daily_data: dict, location: dict):
    df = pl.DataFrame(daily_data)
    df = df.with_columns(
        [
            pl.lit(location["city"]).alias("city"),
            pl.lit(location["country"]).alias("country"),
        ]
    )
    return df


def upload_polars_to_bigquery(df: pl.DataFrame, table_id: str):
    client = bigquery.Client()
    df_pd = df.to_pandas()
    job = client.load_table_from_dataframe(
        df_pd,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    print(f"Uploaded {len(df)} rows to {table_id}")


if __name__ == "__main__":
    weather = fetch_historical_weather_data(end_date="2025-08-01")

    location = weather["location"]
    project_id = "dbt-weather-demo"
    dataset = "dbt_weather_dataset"

    # Flatten hourly + daily
    df_hourly = flatten_hourly(weather["hourly_data"], location)
    df_daily = flatten_daily(weather["daily_data"], location)

    upload_polars_to_bigquery(df_hourly, f"{project_id}.{dataset}.raw_hourly_weather")
    upload_polars_to_bigquery(df_daily, f"{project_id}.{dataset}.raw_daily_weather")
