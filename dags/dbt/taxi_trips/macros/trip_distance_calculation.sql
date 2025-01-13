{%- macro calculate_trip_distance() -%}

3959 * ACOS(
        COS(RADIANS(pickup_latitude)) * COS(RADIANS(dropoff_latitude)) *
        COS(RADIANS(dropoff_longitude) - RADIANS(pickup_longitude)) +
        SIN(RADIANS(pickup_latitude)) * SIN(RADIANS(dropoff_latitude))
      )

{%- endmacro -%}