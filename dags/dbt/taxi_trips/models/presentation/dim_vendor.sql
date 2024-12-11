{{
  config(
    materialized = 'table'
  )
}}

SELECT
    DISTINCT(vendor_name) as id,
    CASE
        WHEN id = 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN id = 2 THEN 'VeriFone Inc'
    END as name,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM {{ ref('taxi_trips_consistent') }}