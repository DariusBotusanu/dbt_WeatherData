{{ config(materialized='view') }}

SELECT
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', time) AS ts,
    SAFE_CAST(temperature_2m AS FLOAT64) AS temperature,
    SAFE_CAST(precipitation AS FLOAT64) AS precipitation,
    SAFE_CAST(relative_humidity_2m AS FLOAT64) AS humidity,
    SAFE_CAST(wind_speed_10m AS FLOAT64) AS wind_speed,
    SAFE_CAST(pressure_msl AS FLOAT64) AS pressure,
    city,
    country
FROM {{ source('dbt_weather_dataset', 'raw_hourly_weather') }}
