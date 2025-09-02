{{ config(materialized='table') }}

WITH daily AS (
    SELECT * FROM {{ ref('stg_daily_weather') }}
),
hourly AS (
    SELECT 
        *
    FROM {{ ref('stg_hourly_weather') }}
)
SELECT
    d.date,
    d.city,
    d.country,
    AVG(h.temperature) AS avg_temperature,
    MAX(h.temperature) AS max_temperature,
    MIN(h.temperature) AS min_temperature,
    SUM(h.precipitation) AS total_precipitation,
    MAX(d.wind_speed_max) AS max_wind_speed,
    AVG(h.humidity) AS avg_humidity,
    AVG(h.pressure) AS avg_pressure
FROM daily d
JOIN hourly h
  ON DATE(h.ts) = d.date
 AND d.city = h.city
GROUP BY d.date, d.city, d.country
ORDER BY d.date DESC
