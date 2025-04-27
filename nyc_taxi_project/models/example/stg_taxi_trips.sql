{{ config(materialized='table') }}

SELECT
  trip_id,
  vendor_id,
  CAST(pickup_datetime AS DATE) AS trip_date,
  passenger_count,
  trip_distance,
  fare_amount
FROM {{ ref('raw_taxi_trips') }}
WHERE
  fare_amount > 0
  AND trip_distance > 0
  AND passenger_count IS NOT NULL