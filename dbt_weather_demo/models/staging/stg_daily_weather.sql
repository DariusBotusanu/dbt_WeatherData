{{ config(materialized='view') }}

SELECT
    DATE(time) AS date,
    SAFE_CAST(temperature_2m_max AS FLOAT64) AS temp_max,
    SAFE_CAST(temperature_2m_min AS FLOAT64) AS temp_min,
    SAFE_CAST(precipitation_sum AS FLOAT64) AS precipitation_sum,
    SAFE_CAST(wind_speed_10m_max AS FLOAT64) AS wind_speed_max,
    city,
    country
FROM {{ source('dbt_weather_dataset', 'raw_daily_weather') }}
