{{
  config(
    materialized = 'incremental',
    merge = True,
    tags=["consistent"]
  )
}}

WITH cleaned_data AS (
  SELECT
    -- Filter out records with all mandatory fields null or zero
    CASE
      WHEN pickup_longitude = 0 AND pickup_latitude = 0 AND trip_distance = 0 THEN 0
      WHEN dropoff_longitude = 0 AND dropoff_latitude = 0 THEN 0
      WHEN pickup_longitude = 0 AND pickup_latitude = 0 THEN 0
      ELSE 1
    END AS is_valid,
    COALESCE(vendor_name::NUMBER(18, 15), 1) AS vendor_name,
    CASE
      WHEN tpep_pickup_datetime IS NULL THEN DATEADD(MINUTE, -((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_dropoff_datetime))
      ELSE TRY_TO_TIMESTAMP(tpep_pickup_datetime)
    END AS tpep_pickup_datetime,
    CASE
      WHEN tpep_dropoff_datetime IS NULL THEN DATEADD(MINUTE, ((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_pickup_datetime))
      WHEN tpep_pickup_datetime > tpep_dropoff_datetime THEN DATEADD(MINUTE, ((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_pickup_datetime))
      WHEN tpep_pickup_datetime = tpep_dropoff_datetime THEN DATEADD(MINUTE, ((trip_distance / 12) * 60), TRY_TO_TIMESTAMP(tpep_dropoff_datetime))
      ELSE TRY_TO_TIMESTAMP(tpep_dropoff_datetime)
    END AS tpep_dropoff_datetime,
    CASE
        WHEN passenger_count = 0 THEN 1
        ELSE TRY_TO_NUMBER(passenger_count)
    END AS passenger_count,
    CASE
      WHEN pickup_longitude = dropoff_longitude AND pickup_latitude = dropoff_latitude AND trip_distance = 0 THEN 0.1
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
    fare_amount::FLOAT AS fare_amount,
    extra::FLOAT AS extra,
    mta_tax::FLOAT AS mta_tax,
    tip_amount::FLOAT AS tip_amount,
    tolls_amount::FLOAT AS tolls_amount,
    improvement_surcharge::FLOAT AS improvement_surcharge,
    total_amount::FLOAT AS total_amount,
    trip_duration_minutes::NUMBER(18, 15) AS trip_duration_minutes,
    trip_speed_mph::NUMBER(18, 15) AS trip_speed_mph,
    created_timestamp AS created_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY
        vendor_name,
        tpep_pickup_datetime,
        pickup_longitude,
        pickup_latitude
      ORDER BY
        created_timestamp DESC
    ) AS row_num
  FROM
    {{ source('taxi_trips', 'taxi_trips_raw') }}
),

taxi_trips_consistent AS (
  SELECT
    *
  FROM
      cleaned_data
  WHERE
    is_valid = 1
    AND row_num = 1
)

SELECT
  *
EXCLUDE (is_valid, row_num)
FROM
  taxi_trips_consistent

{% if is_incremental() %}
  WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ this }})
{% endif %}