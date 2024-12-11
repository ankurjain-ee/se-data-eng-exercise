{{
  config(
    materialized = 'incremental'
  )
}}

SELECT
    md5(vendor_name || tpep_pickup_datetime || pickup_longitude || pickup_latitude) as id,
    vendor_name::NUMBER(1,0) as vendor_id,
    md5(pickup_longitude || pickup_latitude) as pickup_location_id,
    md5 (dropoff_longitude || dropoff_latitude) as dropoff_location_id,
    payment_type as payment_type_id,
    md5(tpep_pickup_datetime) as pickup_date_id,
    md5(tpep_dropoff_datetime) as dropoff_date_id,
    tpep_pickup_datetime as pickup_time,
    tpep_dropoff_datetime as dropoff_time,
    passenger_count,
    trip_distance as trip_distance,
    fare_amount as fare_amount,
    extra as extra,
    mta_tax as mta_tax,
    tip_amount as tip_amount,
    tolls_amount as tolls_amount,
    improvement_surcharge as improvement_surcharge,
    (fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge) as total_amount,
    DateDiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
    CASE
        WHEN DateDiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) < 0 THEN 0
        WHEN DateDiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) = 0 THEN 0
        ELSE trip_distance / (DateDiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) / 60)
    END as trip_speed_mph,
    created_timestamp as created_at

FROM {{ ref('taxi_trips_consistent') }}

{% if is_incremental() %}
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}