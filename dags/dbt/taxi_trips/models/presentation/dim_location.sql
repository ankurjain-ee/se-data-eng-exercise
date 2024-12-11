{{
  config(
    materialized = 'table'
  )
}}

WITH locations AS (
  SELECT
    md5(pickup_longitude || pickup_latitude) AS id,
    pickup_latitude AS latitude,
    pickup_longitude AS longitude,
    created_timestamp AS created_at
  FROM
    {{ ref('taxi_trips_consistent') }}
  UNION
  SELECT
    md5 (dropoff_longitude || dropoff_latitude) AS id,
    dropoff_latitude AS latitude,
    dropoff_longitude AS longitude,
    created_timestamp AS created_at
  FROM
    {{ ref('taxi_trips_consistent') }}
)

SELECT
  id,
  latitude,
  longitude,
  created_at
FROM
  locations