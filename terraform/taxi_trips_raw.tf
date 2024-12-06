resource "snowflake_table" "table" {
  database                    = snowflake_schema.schema.database
  schema                      = snowflake_schema.schema.name
  name                        = "TAXI_TRIPS_RAW"
  comment                     = "RAW Taxi Trips Data"
  cluster_by                  = ["to_date(CREATED_TIMESTAMP)"]
  change_tracking             = true

  column {
    name     = "VENDOR_ID"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TPEP_PICKUP_DATETIME"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TPEP_DROPOFF_DATETIME"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "PASSENGER_COUNT"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TRIP_DISTANCE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "PICKUP_LONGITUDE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "PICKUP_LATITUDE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "RATECODEID"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "STORE_AND_FWD_FLAG"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "DROPOFF_LONGITUDE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "DROPOFF_LATITUDE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "PAYMENT_TYPE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "PAYMENT_TYPE_NAME"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "FARE_AMOUNT"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "EXTRA"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "MTA_TAX"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TIP_AMOUNT"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TOLLS_AMOUNT"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "IMPROVEMENT_SURCHARGE"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TOTAL_AMOUNT"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TRIP_DURATION_MINUTES"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name     = "TRIP_SPEED_MPH"
    type     = "VARCHAR(16777216)"
    nullable = true
  }

  column {
    name = "CREATED_TIMESTAMP"
    type = "TIMESTAMP_NTZ(9)"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }

}