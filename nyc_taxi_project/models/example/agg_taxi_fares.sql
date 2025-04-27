{{ config(materialized='table') }}

SELECT
  vendor_id,
  trip_date,
  COUNT(trip_id) AS total_trips,
  SUM(fare_amount) AS total_fare,
  AVG(trip_distance) AS avg_distance
FROM {{ ref('stg_taxi_trips') }}
GROUP BY vendor_id, trip_date