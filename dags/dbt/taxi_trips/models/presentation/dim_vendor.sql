{{
  config(
    materialized = 'incremental',
    tags=["presentation"]
  )
}}

with vendor as (
    SELECT
        DISTINCT(vendor_name) as id,
        CASE
            WHEN id = 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN id = 2 THEN 'VeriFone Inc'
        END as name,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at,
        created_timestamp
    FROM {{ ref('taxi_trips_consistent') }}
)

SELECT
    *
EXCLUDE(created_timestamp)
FROM vendor

{% if is_incremental() %}
  WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ ref('taxi_trips_consistent') }})
{% endif %}