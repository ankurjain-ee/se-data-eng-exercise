{%- macro calculate_datetime(
    trip_distance,
    datetime_field
) -%}

    DATEADD(MINUTE, (({{ trip_distance }} / 12) * 60), TRY_TO_TIMESTAMP({{ datetime_field }}))

{%- endmacro -%}