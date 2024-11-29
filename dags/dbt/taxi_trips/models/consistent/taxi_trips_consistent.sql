{{
    config(
        materialized='incremental'
    )
}}

select
    vendor_name::NUMBER(18, 15) as vendor_id,
	tpep_pickup_datetime::TIMESTAMP as tpep_pickup_datetime,
	tpep_dropoff_datetime::TIMESTAMP as tpep_dropoff_datetime,
	passenger_count::NUMBER(18, 15) as passenger_count,
	trip_distance::NUMBER(18, 15) as trip_distance,
	pickup_longitude::NUMBER(18, 15) as pickup_longitude,
	pickup_latitude::NUMBER(18, 15) as pickup_latitude,
	RatecodeID as ratecode_id,
	store_and_fwd_flag as store_and_fwd_flag,
	dropoff_longitude::NUMBER(18, 15) as dropoff_longitude,
	dropoff_latitude::NUMBER(18, 15) as dropoff_latitude,
	payment_type as payment_type,
	payment_type_name as payment_type_name,
	fare_amount::NUMBER(18, 15) as fare_amount,
	extra::NUMBER(18, 15) as extra,
	mta_tax::NUMBER(18, 15) as mta_tax,
	tip_amount::NUMBER(18, 15) as tip_amount,
	tolls_amount::NUMBER(18, 15) as tolls_amount,
	improvement_surcharge::NUMBER(18, 15) as improvement_surcharge,
	total_amount::NUMBER(18, 15) as total_amount,
	trip_duration_minutes::NUMBER(18, 15) as trip_duration_minutes,
	trip_speed_mph::NUMBER(18, 15) as trip_speed_mph,
	created_timestamp as created_timestamp
from {{ source('taxi_trips', 'taxi_trips_raw') }}

{% if is_incremental() %}
    where created_timestamp > (select max(created_timestamp) from {{ this }})
{% endif %}