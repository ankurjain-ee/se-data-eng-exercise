{%- macro filter_mandatory_fields_zero() -%}

    CASE
      WHEN pickup_longitude = 0 AND pickup_latitude = 0 AND trip_distance = 0 THEN 0
      WHEN dropoff_longitude = 0 AND dropoff_latitude = 0 THEN 0
      WHEN pickup_longitude = 0 AND pickup_latitude = 0 THEN 0
      ELSE 1
    END AS is_valid

{%- endmacro -%}