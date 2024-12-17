{{
  config(
    materialized = 'incremental',
    merge = True,
    unique_key = 'id',
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
    id,
    name,
    created_at,
    updated_at,
    created_timestamp
FROM vendor

{% if is_incremental() %}
  WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ this }})
{% endif %}