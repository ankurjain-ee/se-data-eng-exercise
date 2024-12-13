{{
  config(
    materialized = 'incremental'
  )
}}

WITH fct_taxi_trips AS (
    SELECT
        md5(vendor_name || tpep_pickup_datetime || pickup_longitude || pickup_latitude) as id,
        vendor_name::NUMBER(1,0) as vendor_id,
        md5(pickup_longitude || pickup_latitude) as pickup_location_id,
        md5 (dropoff_longitude || dropoff_latitude) as dropoff_location_id,
        payment_type as payment_type_id,
        md5(tpep_pickup_datetime::DATE) as pickup_date_id,
        md5(tpep_dropoff_datetime::DATE) as dropoff_date_id,
        tpep_pickup_datetime::TIME as pickup_time,
        tpep_dropoff_datetime::TIME as dropoff_time,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        DateDiff(minute, pickup_time, dropoff_time) as trip_duration_minutes,
        CASE
            WHEN DateDiff(minute, pickup_time, dropoff_time) < 0 THEN 0
            WHEN DateDiff(minute, pickup_time, dropoff_time) = 0 THEN 0
            ELSE trip_distance / (DateDiff(minute, pickup_time, dropoff_time) / 60)
        END as trip_speed_mph,
        CURRENT_TIMESTAMP as created_at,
        created_timestamp

    FROM {{ ref('taxi_trips_consistent') }}
)

SELECT
    *
EXCLUDE (created_timestamp)
FROM fct_taxi_trips

{% if is_incremental() %}
  WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ ref('taxi_trips_consistent') }})
{% endif %}