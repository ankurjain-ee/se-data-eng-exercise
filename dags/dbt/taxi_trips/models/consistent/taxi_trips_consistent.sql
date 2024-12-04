{{
  config(
    materialized = 'incremental'
  )
}}

WITH cleaned_data AS (
  SELECT
    -- Filter out records with all mandatory fields null or zero
    CASE
      WHEN pickup_longitude != 0 AND pickup_latitude != 0 AND trip_distance != 0 THEN 1
      WHEN dropoff_longitude != 0 AND dropoff_latitude != 0 THEN 1
      ELSE 0
    END AS is_valid,
    COALESCE(vendor_name::NUMBER(18, 15), 1) AS vendor_id,
    CASE
      WHEN tpep_pickup_datetime IS NULL THEN DATEADD(MINUTE, -((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_dropoff_datetime))
      ELSE TRY_TO_TIMESTAMP(tpep_pickup_datetime)
    END AS tpep_pickup_datetime,
    CASE
      WHEN tpep_dropoff_datetime IS NULL THEN DATEADD(MINUTE, ((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_pickup_datetime))
      WHEN tpep_pickup_datetime > tpep_dropoff_datetime THEN DATEADD(MINUTE, ((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_pickup_datetime))
      ELSE TRY_TO_TIMESTAMP(tpep_dropoff_datetime)
    END AS tpep_dropoff_datetime,
    CASE
        WHEN passenger_count = 0 THEN 1
        ELSE TRY_TO_NUMBER(passenger_count)
    END AS passenger_count,
    CASE
      WHEN trip_distance = 0 THEN 3959 * ACOS(
        COS(RADIANS(pickup_latitude)) * COS(RADIANS(dropoff_latitude)) *
        COS(RADIANS(dropoff_longitude) - RADIANS(pickup_longitude)) +
        SIN(RADIANS(pickup_latitude)) * SIN(RADIANS(dropoff_latitude))
      )
      ELSE trip_distance
    END AS trip_distance,
    CASE
      WHEN pickup_longitude IS NULL AND pickup_latitude IS NULL THEN -73.7781
      ELSE TRY_TO_NUMBER(pickup_longitude, 18, 15)
    END AS pickup_longitude,
    CASE
      WHEN pickup_longitude IS NULL AND pickup_latitude IS NULL THEN 40.6413
      ELSE TRY_TO_NUMBER(pickup_latitude, 18, 15)
    END AS pickup_latitude,
    RatecodeID AS ratecode_id,
    store_and_fwd_flag AS store_and_fwd_flag,
    dropoff_longitude::NUMBER(18, 15) AS dropoff_longitude,
    dropoff_latitude::NUMBER(18, 15) AS dropoff_latitude,
    payment_type AS payment_type,
    payment_type_name AS payment_type_name,
    fare_amount::NUMBER(18, 15) AS fare_amount,
    extra::NUMBER(18, 15) AS extra,
    mta_tax::NUMBER(18, 15) AS mta_tax,
    tip_amount::NUMBER(18, 15) AS tip_amount,
    tolls_amount::NUMBER(18, 15) AS tolls_amount,
    improvement_surcharge::NUMBER(18, 15) AS improvement_surcharge,
    total_amount::NUMBER(18, 15) AS total_amount,
    trip_duration_minutes::NUMBER(18, 15) AS trip_duration_minutes,
    trip_speed_mph::NUMBER(18, 15) AS trip_speed_mph,
    created_timestamp AS created_timestamp
  FROM
    {{ source('taxi_trips', 'taxi_trips_raw') }}
),

duplicates AS (
  SELECT
    vendor_id,
    tpep_pickup_datetime,
    pickup_longitude,
    pickup_latitude
  FROM
    cleaned_data
  GROUP BY
    vendor_id,
    tpep_pickup_datetime,
    pickup_longitude,
    pickup_latitude
  HAVING
    COUNT(*) > 1
),

taxi_trips_consistent AS (
  SELECT
    *
  FROM
      cleaned_data
  WHERE
    is_valid = 1
    AND (vendor_id, tpep_pickup_datetime, pickup_longitude, pickup_latitude) NOT IN (
      SELECT
        vendor_id,
        tpep_pickup_datetime,
        pickup_longitude,
        pickup_latitude
      FROM
          duplicates
    )
)

SELECT
  *
FROM
  taxi_trips_consistent
WHERE
  is_valid = 1

{% if is_incremental() %}
  AND created_timestamp > (SELECT MAX(created_timestamp) FROM {{ this }})
{% endif %}