{{
    config(
        materialized='incremental'
    )
}}

select
    vendor_name as vendor_id,
	tpep_pickup_datetime as tpep_pickup_datetime,
	tpep_dropoff_datetime as tpep_dropoff_datetime,
	passenger_count as passenger_count,
	trip_distance as trip_distance,
	pickup_longitude as pickup_longitude,
	pickup_latitude as pickup_latitude,
	RatecodeID as ratecode_id,
	store_and_fwd_flag as store_and_fwd_flag,
	dropoff_longitude as dropoff_longitude,
	dropoff_latitude as dropoff_latitude,
	payment_type as payment_type,
	payment_type_name as payment_type_name,
	fare_amount as fare_amount,
	extra as extra,
	mta_tax as mta_tax,
	tip_amount as tip_amount,
	tolls_amount as tolls_amount,
	improvement_surcharge as improvement_surcharge,
	total_amount as total_amount,
	trip_duration_minutes as trip_duration_minutes,
	trip_speed_mph as trip_speed_mph,
	created_timestamp as created_timestamp
from {{ source('taxi_trips', 'taxi_trips_raw') }}

{% if is_incremental() %}
    where created_timestamp > (select max(created_timestamp) from {{ this }})
{% endif %}