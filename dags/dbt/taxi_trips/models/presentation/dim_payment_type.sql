{{
  config(
    materialized = 'incremental',
    merge = True,
    unique_key = 'id',
    tags=["presentation"]
  )
}}

with payment_type as (
    SELECT
        DISTINCT payment_type as id,
        CASE
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 5 THEN 'Unknown'
            WHEN payment_type = 6 THEN 'Voided trip'
        END AS name,
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
FROM payment_type

{% if is_incremental() %}
  WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ this }})
{% endif %}