{{
  config(
    materialized = 'view',
    tags=["presentation"]
  )
}}


WITH weekly_trips AS (
  SELECT
    date.week as week,
    date.month as month,
    date.year as year,
    COUNT(fct.id) AS week_total_trips
  FROM
    fct_taxi_trips fct
  JOIN
    dim_date date ON fct.pickup_date_id = date.id
  GROUP BY
    date.week,
    date.month,
    date.year
),

weekly_growth AS (
  SELECT
    week,
    month,
    year,
    week_total_trips,
    LAG(week_total_trips) OVER (ORDER BY year,month, week) AS prev_week_trips
  FROM
    weekly_trips
)

SELECT
  week,
  month,
  year,
  week_total_trips,
  ROUND((week_total_trips - prev_week_trips) / prev_week_trips * 100, 5) AS week_growth_rate
FROM
  weekly_growth
WHERE
  prev_week_trips IS NOT NULL
ORDER BY week