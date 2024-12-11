{{
  config(
    materialized = 'table'
  )
}}

WITH dates AS (
SELECT
    md5(tpep_pickup_datetime) as id,
    tpep_pickup_datetime as datetime,
    YEAR(datetime) as year,
    MONTH(datetime) as month,
    DAY(datetime) as day,
    created_timestamp as created_at
FROM {{ ref('taxi_trips_consistent') }}

UNION

SELECT
    md5(tpep_dropoff_datetime) as id,
    tpep_dropoff_datetime as datetime,
    YEAR(datetime) as year,
    MONTH(datetime) as month,
    DAY(datetime) as day,
    created_timestamp as created_at
FROM {{ ref('taxi_trips_consistent') }}
)

SELECT
    id,
    datetime,
    year,
    month,
    day,
    created_at
FROM dates