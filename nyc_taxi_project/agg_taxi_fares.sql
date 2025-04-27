SELECT
  vendor_id,
  trip_date,
  COUNT(trip_id) AS total_trips,
  SUM(fare_amount) AS total_fare,
  AVG(trip_distance) AS avg_distance
FROM (
  SELECT
    trip_id,
    vendor_id,
    CAST(pickup_datetime AS DATE) AS trip_date,
    passenger_count,
    trip_distance,
    CASE
      WHEN fare_amount > 0 THEN fare_amount
      ELSE NULL
    END AS fare_amount
  FROM raw_taxi_trips
  WHERE
    fare_amount > 0
    AND trip_distance > 0
    AND passenger_count IS NOT NULL
) AS stg_taxi_trips
GROUP BY vendor_id, trip_date