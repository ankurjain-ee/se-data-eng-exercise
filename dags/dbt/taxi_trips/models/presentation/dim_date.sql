{{
  config(
    materialized = 'incremental'
  )
}}

WITH dates AS (
SELECT
    md5(tpep_pickup_datetime::DATE) as id,
    tpep_pickup_datetime::DATE as full_date,
    YEAR(full_date) as year,
    MONTH(full_date) as month,
    DAY(full_date) as day,
    CURRENT_TIMESTAMP as created_at,
    created_timestamp
FROM {{ ref('taxi_trips_consistent') }}

UNION

SELECT
    md5(tpep_dropoff_datetime::DATE) as id,
    tpep_dropoff_datetime::DATE as full_date,
    YEAR(full_date) as year,
    MONTH(full_date) as month,
    DAY(full_date) as day,
    CURRENT_TIMESTAMP as created_at,
    created_timestamp
FROM {{ ref('taxi_trips_consistent') }}
)

SELECT
    id,
    full_date,
    year,
    month,
    day,
    created_at
FROM dates

{% if is_incremental() %}
  WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ ref('taxi_trips_consistent') }})
{% endif %}